require './yahoo_rest'

RSpec.describe YahooRest, '#response' do
  context 'When given an existing symbol' do
    it 'should report that the symbol is existing' do
      yr = YahooRest.new
      # Apple should exist, at least for now.
      response = yr.check_existance('AAPL')
      expect(response).to eq true
    end
  end
end

RSpec.describe YahooRest, '#response' do
  context 'When giving a non existing symbol' do
    it 'should report N/A when asking for the ask price' do
      yr = YahooRest.new
      response = yr.request_ask('ASHKJDH')
      expect(response.strip!).to eq 'N/A'
    end
  end
end
