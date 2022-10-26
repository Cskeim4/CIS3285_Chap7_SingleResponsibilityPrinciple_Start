using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SingleResponsibilityPrinciple
{
    public class TradeProcessor
    {

        //Split the code into methods(parts) with one function, refactor for clarity

        public void ProcessTrades(Stream stream)
        {
            IEnumerable<string> lines;

            //Call ReadTradeData method
            lines = ReadTradeData(stream);

            //Call ParseTrades method
            List<TradeRecord> trades = ParseTrades(lines);

            //Call StoreTrades method
            StoreTrades(trades);

        }

        //Reads the trade data from the stream
        private IEnumerable<string> ReadTradeData(Stream stream)
        {
            // read rows
            var lines = new List<string>();

            using (var reader = new StreamReader(stream))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    lines.Add(line);
                }
            }

            return lines;
        }

        //Parse data into parts read in from trades
        private List<TradeRecord> ParseTrades(IEnumerable<string> lines)
        {
            var trades = new List<TradeRecord>();
            var lineCount = 1;

            foreach (var line in lines)
            {
                var fields = line.Split(new char[] { ',' });

                //Call the method to validate the data
                //Skip over lines with errors using continue
                if (ValidateTradeData(fields, lineCount))
                {
                    //Calculate the values
                    TradeRecord trade = MapTradeDataToTradeRecord(fields);
                    trades.Add(trade);

                }

                lineCount++;
            }

            return trades;
        }

        //Validate text line
        //Return false if there is an error
        //Return true if line is okay
        private bool ValidateTradeData(string[] fields, int lineCount)
        {
            if (fields.Length != 3)
            {
                //First argument line count is put in {0} and second argument fields.length is put in {1}
                LogMessage("WARN: Line {0} malformed. Only {1} field(s) found.", lineCount, fields.Length);
                return false;
            }

            if (fields[0].Length != 6)
            {
                LogMessage("WARN: Trade currencies on line {0} malformed: '{1}'", lineCount, fields[0]);
                return false;
            }

            int tradeAmount;
            if (!int.TryParse(fields[1], out tradeAmount)) //parse to validate
            {
                LogMessage("WARN: Trade amount on line {0} not a valid integer: '{1}'", lineCount, fields[1]);
                return false;
            }

            decimal tradePrice;
            if (!decimal.TryParse(fields[2], out tradePrice)) //parse to validate
            {
                LogMessage("WARN: Trade price on line {0} not a valid decimal: '{1}'", lineCount, fields[2]);
                return false;
            }

            return true;
        }

        //Pull out console write lines, and call log message to send the messages to a log(file)
        private void LogMessage(string message, params object[] args)
        {
            Console.WriteLine(message, args);
        }

        private TradeRecord MapTradeDataToTradeRecord(string[] fields)
        {
            string sourceCurrencyCode = fields[0].Substring(0, 3);
            string destinationCurrencyCode = fields[0].Substring(3, 3);

            int tradeAmount = int.Parse(fields[1]); //parse to actually parse the data
            decimal tradePrice = decimal.Parse(fields[2]); //parse to actually parse the data

            // calculate values
            TradeRecord tradeRec = new TradeRecord
            {
                SourceCurrency = sourceCurrencyCode,
                DestinationCurrency = destinationCurrencyCode,
                Lots = tradeAmount / LotSize,
                Price = tradePrice
            };
            return tradeRec;
        }

        //Store trade records in a database
        private void StoreTrades(List<TradeRecord> trades)
        {
            using (var connection = new System.Data.SqlClient.SqlConnection("Data Source=(local);Initial Catalog=TradeDatabase;Integrated Security=True;"))
            {
                connection.Open();
                using (var transaction = connection.BeginTransaction())
                {
                    foreach (var trade in trades)
                    {
                        //Create the command
                        var command = connection.CreateCommand();
                        command.Transaction = transaction;
                        command.CommandType = System.Data.CommandType.StoredProcedure;
                        command.CommandText = "dbo.insert_trade";
                        //Add parameters separately to protect against SQL injection
                        command.Parameters.AddWithValue("@sourceCurrency", trade.SourceCurrency);
                        command.Parameters.AddWithValue("@destinationCurrency", trade.DestinationCurrency);
                        command.Parameters.AddWithValue("@lots", trade.Lots);
                        command.Parameters.AddWithValue("@price", trade.Price);

                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                }
                connection.Close();

                Console.WriteLine("INFO: {0} trades processed", trades.Count);
            }
        }

        private static float LotSize = 100000f;
    }
}