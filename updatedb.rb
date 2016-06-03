require 'pg'
require_relative 'yahoo_rest'

class UpdatePrices
  def initialize(dbname)
    @conn = PG.connect(dbname: dbname)
    @yr = YahooRest.new
  end

  def update
    y = @conn.exec('SELECT instr_id FROM instruments')
    y.each do |row|
      updateOne(row['instr_id'])
    end
  end

  def updateOne(id)
    @conn.exec("UPDATE instruments SET ask = #{@yr.request_ask(id)}
		WHERE instr_id = '#{id}'")
    @conn.exec("UPDATE instruments SET bid = #{@yr.request_bid(id)}
		WHERE instr_id = '#{id}'")
  end
end

up = UpdatePrices.new 'summitdb'
up.update
