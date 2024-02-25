require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "minitest", require: "minitest/autorun"
  gem "pg"
end

require "csv"
require "json"

$data_rows_in_order =
  lambda do
    Minitest::Test.i_suck_and_my_tests_are_order_dependent!
    CSV.parse(
      <<~SPEC,
        scenario_name              | reader_name       | expected_result

        mk_no_overlap_scenario     | simple_reader     | [1, 2, 3]
        mk_no_overlap_scenario     | xmin_id_reader    | [1, 2, 3]
        mk_no_overlap_scenario     | xmin_txid_reader  | [1, 2, 3]
        mk_no_overlap_scenario     | share_lock_reader | [1, 2, 3]

        mk_simple_overlap_scenario | simple_reader     | [1, 2, 3]
        mk_simple_overlap_scenario | xmin_id_reader    | [1, 2, 3]
        mk_simple_overlap_scenario | xmin_txid_reader  | [1, 2, 3]
        mk_simple_overlap_scenario | share_lock_reader | [1, 2, 3]

        mk_tricky_overlap_scenario | simple_reader     | [1, 2, 3]
        mk_tricky_overlap_scenario | xmin_id_reader    | [1, 2, 3]
        mk_tricky_overlap_scenario | xmin_txid_reader  | [1, 3, 2]
        mk_tricky_overlap_scenario | share_lock_reader | [1, 2, 3]
    SPEC
      col_sep: "|",
      skip_blanks: true,
      strip: true,
      headers: true,
      return_headers: false
    ).each
  end.call

class LogTest < Minitest::Test
  $data_rows_in_order.each do |row|
    define_method("test_#{row["scenario_name"]}_#{row["reader_name"]}") do
      mk_test(
        send(row["scenario_name"]),
        send(row["reader_name"]),
        JSON.parse(row["expected_result"])
      )
    end
  end

  private

  # baba:  B 1 C
  # kaka:        B 2  C
  # dudu:         B 3   C
  # query:           Q Q Q
  def mk_no_overlap_scenario
    baba, kaka, dudu = mk_actor(1), mk_actor(1), mk_actor(1)
    Fiber.new do
      3.times { baba.resume }
      [kaka, dudu, kaka, dudu].each { _1.resume }
      Fiber.yield

      kaka.resume
      Fiber.yield

      dudu.resume
    end
  end

  # baba:  B 1 C
  # kaka:        B   3 C
  # dudu:         B 2    C
  # query:            Q Q Q
  def mk_simple_overlap_scenario
    baba, kaka, dudu = mk_actor(1), mk_actor(1), mk_actor(1)
    Fiber.new do
      3.times { baba.resume }
      [kaka, dudu, dudu, kaka].each { _1.resume }
      Fiber.yield

      kaka.resume
      Fiber.yield

      dudu.resume
    end
  end

  # kaka:      B 2    C
  # dudu:   B 1   3 C
  # query:         Q Q Q
  def mk_tricky_overlap_scenario
    kaka, dudu = mk_actor(1), mk_actor(2)
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
        SELECT id, txid::text
        FROM log 
        WHERE id > $1
        ORDER BY id
      SQL
    end
  end

  def xmin_id_reader
    lambda do |connection, last_id, last_txid|
      rows = connection.exec_params(<<~SQL, [last_id]).to_a
        SELECT id, txid::text
        FROM log 
        WHERE id > $1 AND txid < pg_snapshot_xmin(pg_current_snapshot())
        ORDER BY txid, id
      SQL
    end
  end

  def xmin_txid_reader
    lambda do |connection, last_id, last_txid|
      rows = connection.exec_params(<<~SQL, [last_txid]).to_a
        SELECT id, txid::text
        FROM log 
        WHERE txid > $1::xid8 AND txid < pg_snapshot_xmin(pg_current_snapshot())
        ORDER BY txid, id
      SQL
    end
  end

  def share_lock_reader
    lambda do |connection, last_id, last_txid|
      connection.exec("BEGIN")
      connection.exec("LOCK TABLE log IN SHARE MODE NOWAIT")
      rows = connection.exec_params(<<~SQL, [last_id]).to_a
        SELECT id, txid::text
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

  def mk_test(scenario, reader, expected_result)
    consumer = mk_consumer

    run_lifecycle do
      3.times do
        scenario.resume
        consumer.call(reader)
      end

      assert_equal expected_result, consumer.result
    end
  end

  def run_lifecycle
    connection = mk_connection.call
    connection.exec(<<~SQL)
      CREATE TABLE log (
        id   serial      primary key,
        evid uuid        not null default gen_random_uuid(),
        txid xid8        not null default pg_current_xact_id(),
        time timestamptz not null default now()
      )
    SQL
    yield connection
  rescue PG::QueryCanceled => e
  ensure
    connection.exec("DROP TABLE log")
  end

  def mk_connection
    lambda do
      connection = PG.connect(dbname: "log")
      connection.type_map_for_results =
        PG::BasicTypeMapForResults.new(connection)
      connection.type_map_for_queries =
        PG::BasicTypeMapForQueries.new(connection)
      connection.exec("SET idle_in_transaction_session_timeout = 1000")
      connection
    end
  end

  def mk_actor(row_count)
    Fiber.new do
      connection = mk_connection.call
      connection.exec("BEGIN")
      Fiber.yield
      row_count.times do
        connection.exec("INSERT INTO log DEFAULT VALUES")
        Fiber.yield
      end
      connection.exec("COMMIT")
    end
  end

  def mk_consumer = Consumer.new(mk_connection.call)

  class Consumer
    attr_reader :result

    def initialize(connection)
      @connection = connection
      @last_id, @last_txid = 0, 0
      @result = []
    end

    def call(implementation)
      rows = implementation.call(@connection, @last_id, @last_txid)
      return if rows.empty?

      @last_id, @last_txid = rows.last.values_at("id", "txid")
      @result += rows.map { _1["id"] }
    end
  end
end
