require 'aws-sdk'
require 'pry'
return unless __FILE__ == $PROGRAM_NAME

table_name = 'ds_test_menu_items'
batch_put_slice = 0..24


Aws.config[:credentials] = Aws::SharedCredentials.new
Aws.config[:region] = 'us-east-1'

# point to DynamoDB Local, comment out this line to use real DynamoDB
#Aws.config[:dynamodb] = { endpoint: 'http://localhost:8993' }

dynamodb = Aws::DynamoDB::Client.new

begin
  dynamodb.describe_table(table_name: table_name)
  puts "Table #{table_name} Exists - exit!"
  exit!

rescue Aws::DynamoDB::Errors::ResourceNotFoundException
  puts "Creating table #{table_name}"
  dynamodb.create_table(
    table_name: table_name,
    attribute_definitions: [
      {
        attribute_name: 'store_pretty_url',
        attribute_type: 'S'
      },
      {
        attribute_name: 'date',
        attribute_type: 'S'
      }
    ],
    key_schema: [
      {
        attribute_name: 'store_pretty_url',
        key_type: 'HASH'
      },
      {
        attribute_name: 'date',
        key_type: 'RANGE'
      }
    ],
    provisioned_throughput: {
      read_capacity_units: 1000,
      write_capacity_units: 1000,
    }
  )

  # wait for table to be available
  puts "Waiting for table #{table_name} to be available..."
  dynamodb.wait_until(:table_exists, table_name: table_name)
  puts "table #{table_name} created!"
end

# Define an exit block to cleanup
at_exit do
  puts "Cleaning up and dropping the table #{table_name}"
  dynamodb.delete_table(table_name: table_name)
  puts "All Clean!"
end


# Do some stuff
class LuckyArray < Array
  def sample
    fail IndexError, 'Can not sample empty array' if self.empty?
    self[@prng.rand((0..self.size-1))]
  end

  def sample!
    fail IndexError, 'Can not sample empty array' if self.empty?
    delete_at(@prng.rand((0..self.size-1)))
  end

  def initialize(args, prng = Random.new, &block)
    @prng = prng
    super(args, &block)
  end
end

nstores = 3
ndays = 100
nitems = 50..100

class DataGenerator

  attr_accessor :start_date, :end_date, :items_by_store

  def initialize(nstores, ndays, nitems)
    @nstores = nstores
    @ndays = ndays
    @nitems = nitems
    @prng = Random.new(1234)

    s1 = ['bobs','bills','janes','johns','jennys','blakes']
    s2 = ['country','city','suburban','rural','pastural','ghetto']
    s3 = ['bunker','shelter','nest','mansion','home','yard','doghouse']

    @stores = LuckyArray.new( s1.flat_map{ |a| s2.flat_map{ |b| s3.map{ |c| "#{a}_#{b}_#{c}" } } }, prng)

    i1 = ['ch','st','gh','ph','tr','dr','fr','wh','cl']
    i2 = ['ee','ea','a','e','i','o','u','ou','ie']
    i3 = ['foo','bar','baz','fud','hug','por','nor','low']

    @item_names = i1.flat_map{ |a| i2.flat_map{ |b| i3.map{ |c| "#{a}#{b}#{c}" } } }

    # Save the data to use in queries...
    @start_date = Date.new(2016,1,18)
    @end_date = Date.new(2016,1,18) + ndays - 1

    @items_by_store = Hash.new { |hash, key| hash[key] = [] }

  end

  def store_day_item_data

    nstores.times.flat_map do
      store = stores.sample!

      ndays.times.flat_map do |d|
        date = (start_date + d).to_s

        this_days_names = LuckyArray.new(item_names , prng)

        prng.rand(nitems).times.map do
          units = prng.rand(1..300)
          gross = units * (prng.rand * 100 + 2)
          pnu = prng.rand

          item_name = this_days_names.sample!
          items_by_store[store] << item_name
          {
            store_pretty_url: store,
            date: date,
            item: item_name,
            units: units,
            gross: gross,
            nu: (units * pnu).to_i,
            repeat: (units * (1 - pnu)).to_i
          }
        end
      end
    end
  end

  def store_day_data

    nstores.times.flat_map do
      store = stores.sample!

      ndays.times.map do |d|
        date = (start_date + d).to_s

        this_days_names = LuckyArray.new(item_names , prng)

        item_data = Hash[
          prng.rand(nitems).times.map do
            units = prng.rand(1..300)
            gross = units * (prng.rand * 100 + 2)
            pnu = prng.rand

            item_name = this_days_names.sample!
            items_by_store[store] << item_name

            [
              item_name,
              {
                units: units,
                gross: gross,
                nu: (units * pnu).to_i,
                repeat: (units * (1 - pnu)).to_i
              }
            ]
          end
        ]

        {
          store_pretty_url: store,
          date: date
        }.merge(item_data)
      end
    end
  end


  private
  attr_reader :nitems, :item_names, :nstores, :stores, :ndays, :prng
end

puts 'Generating data!'
dg = DataGenerator.new(nstores, ndays, nitems)

data = dg.store_day_data

puts "Made #{data.size} dbitems... batch putting now..."

dbitems = data.slice!(batch_put_slice)
until dbitems.empty?
  request = {
    request_items: { # required
      table_name => dbitems.map do |dbitem|
        { put_request: { item: dbitem} }
      end
    },
    return_consumed_capacity: "INDEXES",
    return_item_collection_metrics: "SIZE"
  }

  resp = dynamodb.batch_write_item(request)

  puts resp.to_h.pretty_inspect
  fail unless resp[:unprocessed_items].empty?

  dbitems = data.slice!(batch_put_slice)
end

puts "Lets do some queries!"

dg.items_by_store.keys.each do |store|
  tn = Time.now
  resp = dynamodb.query(
    table_name: table_name,
    key_condition_expression: "store_pretty_url = :store",
    expression_attribute_values: { ":store" => store },
    return_consumed_capacity: 'TOTAL'
    )
  puts "Got all time query result in: #{Time.now - tn} seconds for store: #{store}"
  results = resp.to_h
  results.delete(:items)
  puts results.inspect
end


dg.items_by_store.keys.each do |store|
  item = dg.items_by_store[store][1..20]

  tn = Time.now
  resp = dynamodb.query(
    table_name: table_name,
    projection_expression: item.join(','),
    key_condition_expression: "store_pretty_url = :store",
    expression_attribute_values: { ":store" => store },
    return_consumed_capacity: 'TOTAL'
    )
  puts "Got all time query for 20 items in: #{Time.now - tn} seconds for store: #{store}"
  results = resp.to_h
  results.delete(:items)
  puts results.inspect
end

dg.items_by_store.keys.each do |store|
  tn = Time.now
  resp = dynamodb.query(
    table_name: table_name,
    key_condition_expression: "store_pretty_url = :store AND #D = :day",
    expression_attribute_names: {'#D'=>'date'},
    expression_attribute_values: { ":store" => store, ':day' => dg.start_date.to_s },
    return_consumed_capacity: 'TOTAL'
    )
  puts "Got query result for one day in: #{Time.now - tn} seconds for store: #{store}"
  results = resp.to_h
  results.delete(:items)
  puts results.inspect
end

dg.items_by_store.keys.each do |store|
  sday = dg.start_date.to_s
  eday = (dg.start_date + ndays/2).to_s
  tn = Time.now
  resp = dynamodb.query(
    table_name: table_name,
    key_condition_expression: "store_pretty_url = :store AND #D BETWEEN :sday AND :eday",
    expression_attribute_names: {'#D'=>'date'},
    expression_attribute_values: { ":store" => store, ':sday' => sday, ':eday' => eday },
    return_consumed_capacity: 'TOTAL'
    )
  puts "Got range query result for #{ndays/2} in: #{Time.now - tn} seconds for store: #{store}"
  results = resp.to_h
  results.delete(:items)
  puts results.inspect
end


dg.items_by_store.keys.each do |store|
  sday = dg.start_date.to_s
  eday = (dg.start_date + ndays).to_s
  tn = Time.now
  resp = dynamodb.query(
    table_name: table_name,
    key_condition_expression: "store_pretty_url = :store AND #D BETWEEN :sday AND :eday",
    expression_attribute_names: {'#D'=>'date'},
    expression_attribute_values: { ":store" => store, ':sday' => sday, ':eday' => eday },
    return_consumed_capacity: 'TOTAL'
    )
  puts "Got range query result for #{ndays} in: #{Time.now - tn} seconds for store: #{store}"
  results = resp.to_h
  results.delete(:items)
  puts results.inspect
end

puts 'Tada - All done!'
