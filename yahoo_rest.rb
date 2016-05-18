require 'rest-client'
def doRequest(params)
  RestClient.get('http://download.finance.yahoo.com/d/quotes.csv',
      { :params => params }
    ) do |response|
     return response
    end
end

def requestName(symbol)
  response = doRequest({:s => symbol, :f => 'n'})
  return response
end

def requestBid(symbol)
  response = doRequest({:s => symbol, :f => 'b'})
  return response
end

def requestAsk(symbol)
  response = doRequest({:s => symbol, :f => 'a'})
  return response
end

def checkExistance(symbol)
  response = doRequest({:s => symbol, :f => 'n'})
  return !(response.eql? "N/A\n")
end
