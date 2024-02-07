# frozen_string_literal: true

require "bundler/inline"

gemfile do
  source "https://rubygems.org"
  gem "sequel"
  gem "pg"
  gem "benchmark-ips"
end

require "benchmark/ips"
require "sequel"
require "json"
require "securerandom"

DB = Sequel.connect(ENV.fetch("DATABASE_URL"))
DB.create_table?(:log) do
  primary_key :id
  column :data, "jsonb", null: false
  column :type, "varchar", null: false
  column :evid, "uuid", null: false
  column :time, "timestamp", null: false
end

baseline =
  lambda do
    DB[:log].insert(
      data: JSON.dump({ "kaka" => "dudu" }),
      type: "kaka.dudu",
      evid: SecureRandom.uuid,
      time: Sequel.lit("NOW()")
    )
  end

locking =
  lambda do
    DB.transaction do
      DB["SELECT pg_advisory_xact_lock(1845240511599988039)"]
      DB[:log].insert(
        data: JSON.dump({ "kaka" => "dudu" }),
        type: "kaka.dudu",
        evid: SecureRandom.uuid,
        time: Sequel.lit("NOW()")
      )
    end
  end

examples = { baseline: baseline, locking: locking }

Benchmark.ips do |x|
  examples.each { |name, block| x.report(name) { block.call } }

  x.compare
end
