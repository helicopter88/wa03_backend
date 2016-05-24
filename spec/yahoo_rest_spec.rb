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
