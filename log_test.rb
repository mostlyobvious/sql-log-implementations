require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "minitest", require: "minitest/autorun"
  gem "pg"
end

class LogTest < Minitest::Test
  def test_no_overlap_scenario_simple_reader
    mk_test(mk_no_overlap_scenario, simple_reader) do |consumer|
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  def test_no_overlap_scenario_xmin_reader
    mk_test(mk_no_overlap_scenario, xmin_reader) do |consumer|
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  def test_overlap_scenario_simple_reader
    skip "FAIL"

    mk_test(mk_overlap_scenario, simple_reader) do |consumer|
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  def test_overlap_scenario_xmin_reader
    mk_test(mk_overlap_scenario, xmin_reader) do |consumer|
      assert_equal [1, 2], consumer.consumed_ids
    end
  end

  def test_overlap_more_xmin_reader
    skip "FAIL"

    mk_test(mk_overlap_more_scenario, xmin_reader) do |consumer|
      assert_equal [1, 2, 3], consumer.consumed_ids
    end
  end

  def test_overlap_more_xmin_more_reader
    mk_test(mk_overlap_more_scenario, xmin_more_reader) do |consumer|
      assert_equal [1, 3, 2], consumer.consumed_ids
    end
  end

  def test_overlap_more_share_lock_reader
    mk_test(mk_overlap_more_scenario, share_lock_reader) do |consumer|
      assert_equal [1, 2, 3], consumer.consumed_ids
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

  # kaka:      [ 2    ]
  # dudu:   [ 1   3 ]
  # query:         Q Q Q
  def mk_overlap_more_scenario
    kaka, dudu = mk_actor, mk_actor(insert_times: 2)
    Fiber.new do
      [dudu, dudu, kaka, kaka, dudu].each { _1.resume }
      Fiber.yield

      dudu.resume
      Fiber.yield

      kaka.resume
    end
  end

  def simple_reader
    lambda do |connection, last_id, last_txid|
      rows = connection.exec_params(<<~SQL, [last_id]).to_a
        SELECT id 
        FROM log 
        WHERE id > $1
        ORDER BY id
      SQL
    end
  end

  def xmin_reader
    lambda do |connection, last_id, last_txid|
      rows = connection.exec_params(<<~SQL, [last_id]).to_a
        SELECT id 
        FROM log 
        WHERE id > $1 
          AND xmin::text < pg_snapshot_xmin(pg_current_snapshot())::text
        ORDER BY id
      SQL
    end
  end

  # | id | xmin |
  # | 1  | 4865 |
  # | 2  | 4866 |
  # | 3  | 4865 |
  def xmin_more_reader
    lambda do |connection, last_id, last_txid|
      rows = connection.exec_params(<<~SQL, [last_txid]).to_a
        SELECT id, xmin::text as txid
        FROM log 
        WHERE (xmin::text > $1::text) AND xmin::text < pg_snapshot_xmin(pg_current_snapshot())::text
        ORDER BY xmin::text
      SQL
    end
  end

  def share_lock_reader
    lambda do |connection, last_id, last_txid|
      connection.exec("BEGIN")
      connection.exec("LOCK TABLE log IN SHARE MODE NOWAIT")
      rows = connection.exec_params(<<~SQL, [last_id]).to_a
        SELECT id
        FROM log
        WHERE id > $1
        ORDER BY id
      SQL
      connection.exec("COMMIT")
      rows
    rescue PG::LockNotAvailable
      connection.exec("ROLLBACK")
      []
    end
  end

  def mk_test(scenario, reader)
    consumer = mk_consumer

    run_lifecycle do
      3.times do
        scenario.resume
        consumer.call(reader)
      end

      yield consumer
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

  def mk_actor(insert_times: 1)
    Fiber.new do
      connection = mk_connection.call
      connection.exec("BEGIN")
      Fiber.yield
      insert_times.times do
        connection.exec("INSERT INTO log DEFAULT VALUES")
        Fiber.yield
      end
      connection.exec("COMMIT")
    end
  end

  def mk_consumer = Consumer.new(mk_connection.call)

  class Consumer
    attr_reader :consumed_ids

    def initialize(connection)
      @connection = connection
      @last_id, @last_txid = 0, 0
      @consumed_ids = []
    end

    def call(implementation)
      rows = implementation.call(@connection, @last_id, @last_txid)
      return if rows.empty?

      @last_id = rows.last["id"].to_i
      @last_txid = rows.last["txid"]
      @consumed_ids.concat(rows.map { _1["id"].to_i })
    end
  end
end
