require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "sequel"
  gem "pg"
  gem "concurrent-ruby"
end

require "securerandom"
require "logger"

producer_count = 5
consumer_count = 1
batch_size = 1_000

consumer =
  lambda do
    $barrier.wait
    puts "consumer.#{SecureRandom.hex(2)} started"

    last_id, count = 0, 0

    loop do
      results = $db[:log].where { id > last_id }.order(:id).to_a

      if results.empty?
        last_id == producer_count * batch_size ? break : next
      else
        last_id, count = [results.last[:id], results.size + count]

        print "last_id: #{last_id}, consumed: #{count}/#{producer_count * batch_size}\r"
      end
      sleep 0.001
    end
    puts
  end

xmin_consumer =
  lambda do
    $barrier.wait
    puts "consumer.#{SecureRandom.hex(2)} started"

    last_id, count = 0, 0

    loop do
      results =
        $db[:log]
          .where(
            Sequel.lit(
              "id > ? AND xmin::text < txid_snapshot_xmin(txid_current_snapshot())::text",
              last_id
            )
          )
          .order(:id)
          .to_a

      if results.empty?
        last_id == producer_count * batch_size ? break : next
      else
        last_id, count = [results.last[:id], results.size + count]

        print "last_id: #{last_id}, consumed: #{count}/#{producer_count * batch_size}\r"
      end
      sleep 0.001
    end
    puts
  end

producer =
  lambda do
    $barrier.wait
    puts "producer.#{SecureRandom.hex(2)} started"

    batch_size
      .times
      .with_index(1) do |_, count|
        $db.transaction do
          $db[:log].insert
          sleep rand(0.001..0.003)
        end
      end
  end

serialized_producer =
  lambda do
    $barrier.wait
    puts "producer.#{SecureRandom.hex(2)} started"

    batch_size
      .times
      .with_index(1) do |_, count|
        $db.transaction do
          $db.run("SELECT pg_advisory_xact_lock(2137)")
          $db[:log].insert
          sleep rand(0.001..0.003)
        end
      end
  end

{
  "ordinary producer, ordinary consumer" => [producer, consumer],
  "serialized producer, ordinary consumer" => [serialized_producer, consumer],
  "ordinary producer, xmin consumer" => [producer, xmin_consumer]
}.each do |name, (producer, consumer)|
  puts
  puts "--- #{name} ---"

  $barrier = Concurrent::CyclicBarrier.new(producer_count + consumer_count)

  $db =
    Sequel.connect(
      ENV.fetch("DATABASE_URL"),
      pool: producer_count + consumer_count,
      preconnect: :concurrently
    )
  $db.drop_table?(:log)
  $db.create_table?(:log) { primary_key :id }

  pool = Concurrent::FixedThreadPool.new(producer_count + consumer_count)
  consumer_count.times { pool.post { consumer.call } }
  producer_count.times { pool.post { producer.call } }
  pool.shutdown
  pool.wait_for_termination
end
