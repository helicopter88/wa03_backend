require './db'

db = DatabaseQueries.new 'summitdb'

# Checks that user is not in the table
RSpec.describe DatabaseQueries, '#response' do
  context 'Given user and password' do
    it 'should return false if user does not exist' do
      response = db.check_user('John', 'password')
      expect(response).to eq false
    end
  end
end

# Checks that user is in the table
RSpec.describe DatabaseQueries, '#response' do
  context 'Given user and password' do
    it 'should return true if user does exist' do
      response = db.check_user('admin', 'admin')
      expect(response).to eq true
    end
  end
end

# Checks that user is already in the table
RSpec.describe DatabaseQueries, '#response' do
  context 'Given username,password, capital, concurrency' do
    it 'should return false if user already does exist' do
      response = db.insert_user('admin', 'admin', 'Administrator', 100, 'GBP')
      expect(response).to eq false
    end
  end
end

# Checks that user is inserted to the table
RSpec.describe DatabaseQueries, '#response' do
  context 'Given username,password, capital, concurrency' do
    it 'should return true if user is succesfully inserted to the table' do
      response = db.insert_user('John', '123pasw', 'John', 100, 'GBP')
      db.delete_user('John', '123pasw')
      expect(response).to eq true
    end
  end
end

# Checks that we get correct account currency
RSpec.describe DatabaseQueries, '#response' do
  context 'Given username' do
    it 'should return  user\'s account currency' do
      response1 = db.get_account_currency('dom_usd')
      expect(response1).to eq 'USD'
      response2 = db.get_account_currency('admin')
      expect(response2).to eq 'GBP'
    end
  end
end

# Checks that user doesn't own the instrument
RSpec.describe DatabaseQueries, '#response' do
  context 'Given instrument and username' do
    it 'should return 0 if user does not own particular instrument' do
      response = db.users_instr('admin', 'HappyDay')
      expect(response).to eq 0
    end
  end
end
