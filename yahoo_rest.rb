#!/usr/bin/ruby
require 'rest-client'
require 'json'

class YahooRest
  def parse_tokens(tokens)
    case tokens[0]
    # ask_price symbol
    when 'ask_price'
      return "ap_#{tokens[1]}: #{yr.request_ask(tokens[1])}"
    # bid_price symbol
    when 'bid_price'
      return "bp_#{tokens[1]}: #{yr.request_bid(tokens[1])}"
    # exists symbol
    when 'exists'
      return "e_#{tokens[1]}: #{yr.check_existance(tokens[1])}"
    else
      return 'Yahoo: invalid action'
    end
  end

  # Some basic getters that allow us to easily fetch data from Yahoo finance
  def request_name(symbol)
    response = request(s: symbol, f: 'n')
    # puts response.to_json
    response
  end

  def request_bid(symbol)
    response = request(s: symbol, f: 'b')
    response
  end

  def request_ask(symbol)
    response = request(s: symbol, f: 'a')
    response
  end

  def retrieve_se(symbol)
    response = request(s: symbol, f: 'x')
    response
  end

  def check_existance(symbol)
    response = request(s: symbol, f: 'n')
    # Yahoo Finance returns "N/A" when the symbol does not match anything found
    !(response.eql? "N/A\n")
  end

  private

  # We do not do any sanity check for params
  # As we hope that every method will pass us correct arguments
  def request(params)
    # We may have to do some exception handling in case the request does not succeed
    RestClient.get('http://download.finance.yahoo.com/d/quotes.csv',
                   params: params) do |response|
      return response
    end
  end
end
