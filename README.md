# Apache Storm Stock Price Tracker

### Set up the Spout
 * Extend the BaseRickSpout class
 * Connect to Yahoo Finance API
 * Get stock price, previous day closing price and send to Bolt
 
 
 ### Set up the Bold
  * Extend the BaseBasicBolt class
  * Compute Gain/Loss signal
  * Write data to file
