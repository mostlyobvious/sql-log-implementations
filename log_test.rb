require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "minitest", require: "minitest/autorun"
  gem "pg"
end

class LogTest < Minitest::Test
  def simple_reader
    lambda do |connection, last_id|
      rows =
        connection
          .exec_params(<<~SQL, [last_id])
            SELECT id 
            FROM log 
            WHERE id > $1
            ORDER BY id
          SQL
          .map { |row| row.fetch("id").to_i }
      [rows.last, rows]
    end
  end

  def xmin_reader
    lambda do |connection, last_id|
      rows =
        connection
          .exec_params(<<~SQL, [last_id])
            SELECT id 
            FROM log 
            WHERE id > $1 AND xmin::text < txid_snapshot_xmin(txid_current_snapshot())::text
            ORDER BY id
          SQL
          .map { |row| row.fetch("id").to_i }
      [rows.last, rows]
    end
  end

  def test_no_overlap_scenario_simple_reader
    run_lifecycle do
      scenario, consumer = mk_no_overlap_scenario, mk_consumer

      3.times do
        scenario.resume
        consumer.call(simple_reader)
      end

      assert_equal 2, consumer.last_id
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  def test_overlap_scenario_simple_reader
    run_lifecycle do
      scenario, consumer = mk_overlap_scenario, mk_consumer

      3.times do
        scenario.resume
        consumer.call(simple_reader)
      end

      assert_equal 2, consumer.last_id
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  def test_no_overlap_scenario_xmin_reader
    run_lifecycle do
      scenario, consumer = mk_no_overlap_scenario, mk_consumer

      3.times do
        scenario.resume
        consumer.call(xmin_reader)
      end

      assert_equal 2, consumer.last_id
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  def test_overlap_scenario_xmin_reader
    run_lifecycle do
      scenario, consumer = mk_overlap_scenario, mk_consumer

      3.times do
        scenario.resume
        consumer.call(xmin_reader)
      end

      assert_equal 2, consumer.last_id
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  private

  # kaka:  [ 1  ]
  # dudu:   [ 2   ]
  # query:     Q Q Q
  def mk_no_overlap_scenario
    kaka, dudu = mk_actor, mk_actor
    Fiber.new do
      [kaka, dudu, kaka, dudu].each { _1.resume }
      Fiber.yield

      kaka.resume
      Fiber.yield

      dudu.resume
    end
  end

  # kaka:  [   2 ]
  # dudu:   [ 1    ]
  # query:      Q Q Q
  def mk_overlap_scenario
    kaka, dudu = mk_actor, mk_actor
    Fiber.new do
      [kaka, dudu, dudu, kaka].each { _1.resume }
      Fiber.yield

      kaka.resume
      Fiber.yield

      dudu.resume
    end
  end

  def run_lifecycle
    connection = mk_connection.call
    connection.exec("CREATE TABLE log (id serial primary key, name varchar)")
    yield connection
  rescue PG::QueryCanceled => e
  ensure
    connection.exec("DROP TABLE log")
  end

  def mk_connection
    lambda do
      connection = PG.connect(dbname: "log")
      connection.exec("SET idle_in_transaction_session_timeout = 1000")
      connection
    end
  end

  def mk_actor
    Fiber.new do
      connection = mk_connection.call
      connection.exec("BEGIN")
      Fiber.yield
      connection.exec("INSERT INTO log DEFAULT VALUES")
      Fiber.yield
      connection.exec("COMMIT")
    end
  end

  def mk_consumer = Consumer.new(mk_connection.call)

  class Consumer
    attr_reader :last_id, :consumed_ids

    def initialize(connection)
      @connection = connection
      @last_id = 0
      @consumed_ids = []
    end

    def call(implementation)
      last_id, consumed_ids = implementation.call(@connection, @last_id)
      return if consumed_ids.empty?

      @last_id = last_id
      @consumed_ids.concat(consumed_ids)
    end
  end
end
