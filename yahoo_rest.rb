#!/usr/bin/ruby
require 'rest-client'
require 'json'

class YahooRest
  def parse_tokens(tokens)
    case tokens[0]
    when 'req_name'
      return "name #{jsonify(tokens[1], request_name(tokens[1]))}"
    # ask_price symbol
    when 'ask_price'
      return "ask_price #{jsonify(tokens[1], request_ask(tokens[1]))}"
    # bid_price symbol
    when 'bid_price'
      return "bid_price #{jsonify(tokens[1], request_bid(tokens[1]))}"
    # exists symbol
    when 'exists'
      return "exists #{check_existance(tokens[1]).to_s}"
    else
      return 'Yahoo: invalid action'
    end
  end

  def jsonify(symbol, value)
    {:sym => symbol, :res => value.strip!}.to_json
  end
  # Some basic getters that allow us to easily fetch data from Yahoo finance
  def request_name(symbol)
    request(s: symbol, f: 'n')
  end

  def request_bid(symbol)
    request(s: symbol, f: 'b')
  end

  def request_ask(symbol)
    request(s: symbol, f: 'a')
  end

  def retrieve_se(symbol)
    request(s: symbol, f: 'x')
  end

  def check_existance(symbol)
    response = request(s: symbol, f: 'n')
    # Yahoo Finance returns "N/A" when the symbol does not match anything found
    !(response.eql? "N/A\n")
  end

  private
  
  # Performs the actual rest request
  def rest_req(params) 
    # We may have to do some exception handling in case the request does not succeed
    RestClient.get('http://download.finance.yahoo.com/d/quotes.csv',
                   params: params) do |response|
      return response
    end
  end
  
  # This may cache the results for up to 1 minute
  # We do not do any sanity check for params
  # As we hope that every method will pass us correct arguments
  def request(params)
    @reqs ||= Hash.new {|h, key| h[key] = { :res => rest_req(params), :time => Time.new.to_i } }
    if Time.new.to_i - @reqs[params][:time] > 60
      @reqs[params] = { :res => rest_req(params), :time => Time.new.to_i }
    end
    @reqs[params][:res]
  end
end
