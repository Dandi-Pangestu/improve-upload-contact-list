# frozen_string_literal: true

require 'csv'
require 'open-uri'
require 'get_process_mem'

def get_usage
  GetProcessMem.new.mb
end

def print_usage_before_and_after
  before = get_usage
  yield
  after = get_usage

  puts "Before - MEMORY USAGE(MB): #{ before.round }"
  puts "After - MEMORY USAGE(MB): #{ after.round }"
  puts "Actual - MEMORY USAGE(MB): #{ after.round - before.round }"
end

def print_time_spent
  time = Benchmark.realtime do
    yield
  end

  puts "Time: #{time.round(2)}"
end

def clean_for(row_as_hash, string_columns_array)
  row_as_hash.each do |key, value|
    if string_columns_array.include? key
      row_as_hash[key] = value.to_s.strip
    end
  end
  row_as_hash
end

def build_data_index_contact attr
  {
    id:                     attr[0],
    organization_id:        attr[1],
    phone_number:           attr[2],
    full_name:              attr[3],
    status:                 attr[4],
    error_messages:         JSON.parse(attr[5] || '{}'),
    extra:                  JSON.parse(attr[6] || '{}'),
    created_at:             attr[7],
    updated_at:             attr[8],
    account_uniq_id:        attr[9],
    channel_integration_id: attr[10],
    deleted_at:             attr[11],
    avatar:                 JSON.parse(attr[12] || '{}'),
    is_valid:               attr[13],
    channel:                attr[14],
    contact_handler_id:     attr[15],
    is_contact:             attr[16],
    code:                   attr[17],
    authority:              attr[18],
    is_blocked:             attr[19],
    is_contact_extra:       attr[20]
  }
end

def build_contacts_with_batch(contacts_attrs, organization, contact_list)
  channel_integrations = contacts_attrs.pluck(:channel_integration_id)
  account_uniq_ids = contacts_attrs.pluck(:phone_number)

  # Loads here to avoid N+1 query
  contacts_map = Models::Contact
                   .where(organization: organization, account_uniq_id: account_uniq_ids, channel_integration_id: channel_integrations)
                   .index_by(&:account_uniq_id)

  contacts = contacts_attrs.map do |attrs|
    contact = contacts_map[attrs[:account_uniq_id]]
    if contact.nil?
      contact ||= Models::Contact.new
      contact.id = SecureRandom.uuid
      contact.attributes = attrs
      contact.is_contact = false
    else
      contact.full_name      = attrs[:full_name]
      # contact.extra          = attrs[:extra] # -> info! Unused
      contact.error_messages = attrs[:error_messages]
      contact.is_valid       = attrs[:is_valid]
      contact.status         = attrs[:status]
      contact.channel        = attrs.fetch(:channel, 'unknown')
      contact.extra.delete('')
    end

    # Flag Contact Extra
    contact.contact_extras.build(organization_id: organization.id, contact_list_id: contact_list.id, extra: attrs[:extra])
    contact.is_contact_extra = true

    contact
  end

  # insert batch with activerecord import
  batch_size = (REDIS_R.get('batch_size_upload_contact') || 2_000).to_i
  import_contacts = Models::Contact.import! contacts, batch_size: batch_size, returning: [:id, :organization_id, :phone_number, :full_name, :status, :error_messages, :extra, :created_at, :updated_at, :account_uniq_id, :channel_integration_id, :deleted_at, :avatar, :is_valid, :channel, :contact_handler_id, :is_contact, :code, :authority, :is_blocked, :is_contact_extra], recursive: true, on_duplicate_key_update: [:full_name, :error_messages, :is_valid, :status, :channel, :extra, :is_contact_extra]

  # bulk reindex contacts
  index_body_contacts = import_contacts.results.map { |attr| build_data_index_contact attr }
  bulk_index_raw_data(index_body_contacts, Models::Contact, refresh: true)

  # insert batch contact lists contact
  build_contact_lists_contact = import_contacts.ids.map { |id| Models::ContactListsContact.new({ contact_list_id: contact_list.id, contact_id: id }) }
  Models::ContactListsContact.import! build_contact_lists_contact

  true
rescue => e
  Rollbar.error(e, class: self.class, method: __method__, args: (method(__method__).parameters.map { |_, ctx_arg| { ctx_arg => binding.local_variable_get(ctx_arg) } } rescue 'err parse args*'))
  Failure Hashie::Mash.new({ errors: { results: [{ message: 'Raise Error', row: e.as_json }] } })
end

namespace :benchmark_new_flow do
  task create_contact: :environment do
    include Services::Elasticsearch::BulkIndex

    print_usage_before_and_after do
      print_time_spent do
        phone_number_map = {}
        headers = []
        is_headers_fetched = false
        extracted = []

        contact_list = Models::ContactList.find_by(id: 'ca8ad573-ce14-4efd-820c-38eebec92b34')
        organization = contact_list.organization

        if organization.channel_integrations.unknown.first.nil?
          integration                 = Models::ChannelIntegration.unknown.new
          integration.organization_id = organization.id
          integration.save
        else
          integration = organization.channel_integrations.unknown.first!
        end

        csv_text = open('https://qontak-hub-development.s3.amazonaws.com/uploads/direct/files/7f1313bb-1bd5-4e74-8c26-c99fc4000b51/data.csv')
        CSV.foreach(csv_text, headers: true) do |row|
          headers = row.headers unless is_headers_fetched
          row = row.to_h
          message = []

          continue unless row.each_value.any?(&:present?)

          phone_number_key = row['phone_number'].to_phone rescue ''
          continue unless phone_number_map[phone_number_key].nil?
          phone_number_map[phone_number_key] = true

          row = clean_for row, headers

          if row.select { |k, v| v.nil? || v.blank? }.present?
            message << 'field can\'t be empty'
          end

          row.delete_if { |k, v| k.nil? || k.blank? }

          phone_number = row['phone_number'].to_phone rescue nil
          full_name = row['full_name']
          is_valid = false
          unless phone_number.nil?
            is_valid = phone_number.starts_with?('+') ? phone_number.phone? : "+#{phone_number}".phone?
            row.merge(phone_number: phone_number)
            message << 'invalid phone_number' unless is_valid
          end
          extra = row.except('full_name', 'phone_number')
          extra.delete('')

          extracted << {
            organization:           organization,
            phone_number:           phone_number,
            full_name:              full_name,
            status:                 message.present? ? 'failed' : 'success',
            error_messages:         message.present? ? Hashie::Mash.new({ results: message }).as_json : {},
            extra:                  extra,
            is_valid:               is_valid || false,
            account_uniq_id:        phone_number,
            channel_integration_id: integration.id
          }
        end
        csv_text.close unless csv_text.nil?

        extracted.each_slice(100) { |item| build_contacts_with_batch(item, organization, contact_list) }
      end
    end
  end
end