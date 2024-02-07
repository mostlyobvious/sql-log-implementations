require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "sequel"
  gem "pg"
  gem "concurrent-ruby"
end

require "securerandom"

producer_count = 5
consumer_count = 1
batch_size = 1000

$barrier = Concurrent::CyclicBarrier.new(producer_count + consumer_count)

$db =
  Sequel.connect(
    ENV.fetch("DATABASE_URL"),
    pool: producer_count + consumer_count,
    preconnect: :concurrently
  )
$db.drop_table?(:log)
$db.create_table?(:log) { primary_key :id }

consumer =
  lambda do
    $barrier.wait
    puts "consumer.#{SecureRandom.hex(2)} started"

    last_id, count = 0, 0

    loop do
      results = $db[:log].where { id > last_id }.order(:id).to_a
      next if results.empty?

      last_id, count = [results.last[:id], results.size + count]

      print "expected: #{producer_count * batch_size}, consumed: #{count}\r"
    end
  end

producer =
  lambda do
    $barrier.wait
    puts "producer.#{SecureRandom.hex(2)} started"

    batch_size.times.with_index(1) { |_, count| $db[:log].insert }
  end

producer_pool = Concurrent::FixedThreadPool.new(producer_count)
producer_count.times { producer_pool.post { producer.call } }

consumer_pool = Concurrent::FixedThreadPool.new(consumer_count)
consumer_count.times { consumer_pool.post { consumer.call } }

[producer_pool, consumer_pool].each(&:shutdown).each(&:wait_for_termination)
