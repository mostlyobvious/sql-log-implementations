require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "minitest", require: "minitest/autorun"
  gem "pg"
end

class LogTest < Minitest::Test
  # kaka:  [ 1  ]
  # dudu:   [ 2   ]
  # query:     Q Q Q
  def test_no_overlap
    run_lifecycle do |pg|
      kaka, dudu = mk_actor, mk_actor

      kaka.resume
      dudu.resume
      assert_empty visible_row_ids

      kaka.resume
      assert_equal [1], visible_row_ids

      dudu.resume
      assert_equal [1, 2], visible_row_ids
    end
  end

  # kaka:  [   2 ]
  # dudu:   [ 1    ]
  # query:      Q Q Q
  def test_overlap
    run_lifecycle do |pg|
      kaka, dudu = mk_actor, mk_actor

      dudu.resume
      kaka.resume
      assert_empty visible_row_ids

      kaka.resume
      assert_equal [2], visible_row_ids

      dudu.resume
      assert_equal [1, 2], visible_row_ids
    end
  end

  private

  def visible_row_ids
    pg = mk_connection.call
    pg.exec("SELECT id FROM log").map { |row| row.fetch("id").to_i }
  end

  def run_lifecycle
    pg = mk_connection.call
    pg.exec("CREATE TABLE log (id serial primary key)")
    yield pg
  ensure
    pg.exec("DROP TABLE log")
  end

  def mk_actor
    Fiber.new do
      pg = mk_connection.call
      pg.exec("BEGIN")
      pg.exec("INSERT INTO log DEFAULT VALUES")
      Fiber.yield
      pg.exec("COMMIT")
    end
  end

  def mk_connection = -> { PG.connect(dbname: "log") }
end
