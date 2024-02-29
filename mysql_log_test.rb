require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "minitest", require: "minitest/autorun"
  gem "mysql2"
end

require "csv"
require "json"

class MysqlLogTest < Minitest::Test
  def self.parse_specification(spec) =
    CSV.parse(spec, col_sep: "|", skip_blanks: true, strip: true)

  parse_specification(
    <<~SPEC
      mk_no_overlap_scenario     | simple_reader     | [1, 2, 3]
      mk_no_overlap_scenario     | txid_reader       | [1, 2, 3]

      mk_simple_overlap_scenario | simple_reader     | [1, 2, 3]
      mk_simple_overlap_scenario | txid_reader       | [1, 2, 3]

      mk_tricky_overlap_scenario | simple_reader     | [1, 2, 3]
      mk_tricky_overlap_scenario | txid_reader       | [1, 3, 2]
    SPEC
  ).each do |(scenario_name, reader_name, expected_result)|
    define_method("test_#{scenario_name}_#{reader_name}") do
      scenario, reader = send(scenario_name), send(reader_name)
      consumer = mk_consumer

      run_lifecycle do
        3.times do
          scenario.resume
          consumer.call(reader)
        end

        assert_equal JSON.parse(expected_result), consumer.result
      end
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
    lambda { |connection, last_id, last_txid| connection.query(<<~SQL).to_a }
      SELECT id, txid
      FROM log
      WHERE id > #{last_id}
      ORDER BY id
    SQL
  end

  def txid_reader
    lambda { |connection, last_id, last_txid| connection.query(<<~SQL).to_a }
      SELECT id, txid
      FROM log
      WHERE txid > #{last_txid}
      ORDER BY txid, id
    SQL
  end

  def run_lifecycle
    connection = mk_connection.call
    connection.query(<<~SQL)
      CREATE TABLE log (
        id   bigint AUTO_INCREMENT PRIMARY KEY,
        txid bigint
      )
    SQL
    yield connection
  ensure
    connection.query("DROP TABLE log")
  end

  def mk_connection
    lambda do
      connection = Mysql2::Client.new(database: "log", username: "root")
      connection
    end
  end

  def mk_actor(row_count)
    Fiber.new do
      connection = mk_connection.call
      connection.query("BEGIN")
      Fiber.yield
      row_count.times do
        connection.query("INSERT INTO log VALUES ()")
        connection.query("SELECT SLEEP(0.1)")
        connection.query(<<~SQL)
          UPDATE log 
          SET txid = (
            SELECT trx_id 
            FROM information_schema.innodb_trx 
            WHERE trx_mysql_thread_id = connection_id()
          ) 
          WHERE id = LAST_INSERT_ID()
        SQL
        Fiber.yield
      end
      connection.query("COMMIT")
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
