require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "minitest", require: "minitest/autorun"
  gem "pg"
end

class LogTest < Minitest::Test
  class Actor
    def initialize(mk_connection, name)
      @pg = mk_connection.call
      @name = name
    end

    def begin = @pg.exec("BEGIN")
    def insert = @pg.exec_params("INSERT INTO log (name) VALUES ($1)", [@name])
    def commit = @pg.exec("COMMIT")
  end

  # kaka:  [ 1  ]
  # dudu:   [ 2   ]
  # query:     Q Q Q
  def test_no_overlap
    run_lifecycle do |pg|
      kaka, dudu = mk_actor("kaka"), mk_actor("dudu")

      kaka.begin
      dudu.begin

      kaka.insert
      dudu.insert
      assert_empty visible_rows

      kaka.commit
      assert_equal [{ 1 => "kaka" }], visible_rows

      dudu.commit
      assert_equal [{ 1 => "kaka" }, { 2 => "dudu" }], visible_rows
    end
  end

  # kaka:  [   2 ]
  # dudu:   [ 1    ]
  # query:      Q Q Q
  def test_overlap
    run_lifecycle do |pg|
      kaka, dudu = mk_actor("kaka"), mk_actor("dudu")

      kaka.begin
      dudu.begin

      dudu.insert
      kaka.insert
      assert_empty visible_rows

      kaka.commit
      assert_equal [{ 2 => "kaka" }], visible_rows

      dudu.commit
      assert_equal [{ 1 => "dudu" }, { 2 => "kaka" }], visible_rows
    end
  end

  private

  def visible_rows
    mk_connection
      .call
      .exec("SELECT id, name FROM log")
      .map { |row| { row.fetch("id").to_i => row.fetch("name") } }
  end

  def run_lifecycle
    pg = mk_connection.call
    pg.exec("CREATE TABLE log (id serial primary key, name varchar)")
    yield pg
  rescue PG::QueryCanceled => e
  ensure
    pg.exec("DROP TABLE log")
  end

  def mk_actor(name) = Actor.new(mk_connection, name)

  def mk_connection =
    -> do
      PG
        .connect(dbname: "log")
        .tap do |connection|
          connection.exec("SET idle_in_transaction_session_timeout = 1000")
        end
    end
end
