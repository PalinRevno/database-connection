using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using InternalDataAccess.Properties;

namespace InternalDataAccess
{
    internal static class Connection
    {
        /// <summary>
        /// Gets a new connection from the pool
        /// </summary>
        private static SqlConnection GetConnection()
        {
            SqlConnection response;

            response = new SqlConnection(Settings.Default.ConnectionString);

            return response;
        }

        /// <summary>
        /// Creates only <see cref="SqlDbType.NVarChar"/> parameters
        /// </summary>
        internal static IEnumerable<Tuple<string, SqlDbType, object>> CreateNullParameters(params string[] parameterNames)
        {
            foreach (string name in parameterNames)
            {
                yield return new Tuple<string, SqlDbType, object>(name, SqlDbType.NVarChar, DBNull.Value);
            }
        }

        internal static void ExecuteNonQuery(string command, bool isStoredProcedure, params Tuple<string, SqlDbType, object>[] parameters)
        {
            SqlParameter newParameter = null;

            using (SqlCommand dbCommand = GetConnection().CreateCommand())
            {
                // Initializing the command itself
                dbCommand.CommandText = command;

                // Checking if command type update is required
                if (isStoredProcedure)
                {
                    dbCommand.CommandType = CommandType.StoredProcedure;
                }

                // Initializing the parameters
                foreach (Tuple<string, SqlDbType, object> currentParameter in parameters)
                {
                    // Creating a new parameter from the current input
                    newParameter = new SqlParameter(currentParameter.Item1, currentParameter.Item2);
                    newParameter.Value = currentParameter.Item3;

                    // Appending the parameter to the query
                    dbCommand.Parameters.Add(newParameter);
                }

                try
                {
                    dbCommand.Connection.Open();

                    // Executing the procedure
                    dbCommand.ExecuteNonQuery();

                    dbCommand.Connection.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception(string.Format("Could not execute the requested non-query db command : {0}", ex.Message), ex);
                }
                finally
                {
                    if (dbCommand.Connection.State != ConnectionState.Closed)
                    {
                        dbCommand.Connection.Close();
                    }
                }
            }
        }

        internal static string ExecuteScalarQuery(string command, bool isStoredProcedure, params Tuple<string, SqlDbType, object>[] parameters)
        {
            SqlParameter newParameter = null;
            SqlParameter output = null;
            string responseValue;

            using (SqlCommand dbCommand = GetConnection().CreateCommand())
            {
                // Initializing the command itself
                dbCommand.CommandText = command;

                // Checking if command type update is required
                if (isStoredProcedure)
                {
                    dbCommand.CommandType = CommandType.StoredProcedure;
                }

                // Initializing the parameters
                foreach (Tuple<string, SqlDbType, object> currentParameter in parameters)
                {
                    // Checking if the current parameter is an output parameter(no value)
                    if (currentParameter.Item3 == null)
                    {
                        // Creating an output parameter
                        newParameter = new SqlParameter();
                        newParameter.ParameterName = currentParameter.Item1;
                        newParameter.Direction = ParameterDirection.Output;
                        newParameter.Size = 11;

                        // Storing the parameter for the value
                        output = newParameter;
                    }
                    else
                    {
                        // Creating a new parameter from the current input
                        newParameter = new SqlParameter(currentParameter.Item1, currentParameter.Item2);
                        newParameter.Value = currentParameter.Item3;
                    }

                    // Appending the parameter to the query
                    dbCommand.Parameters.Add(newParameter);
                }

                try
                {
                    dbCommand.Connection.Open();

                    // Checking command type
                    if (isStoredProcedure)
                    {
                        // Executing the procedure
                        dbCommand.ExecuteNonQuery();

                        // Retrieving the response
                        responseValue = output.Value.ToString();
                    }
                    else
                    {
                        // Executing the scalar command
                        responseValue = dbCommand.ExecuteScalar().ToString();
                    }

                    dbCommand.Connection.Close();
                }
                catch (Exception ex)
                {
                    throw new Exception(string.Format("Could not execute the requested db command : {0}", ex.Message), ex);
                }
                finally
                {
                    if (dbCommand.Connection.State != ConnectionState.Closed)
                    {
                        dbCommand.Connection.Close();
                    }
                }
            }

            return responseValue;
        }

        internal static DataTable FillTable(string selectCommand, params Tuple<string, SqlDbType, object>[] parameters)
        {
            #region Variables

            DataTable response;
            SqlDataAdapter queryAdapter;
            SqlParameter newParameter;

            #endregion

            // Initializing the auxiliary adapter
            queryAdapter = new SqlDataAdapter(selectCommand, GetConnection());

            // Initializing the parameters
            foreach (Tuple<string, SqlDbType, object> currentParameter in parameters)
            {
                // Creating a new parameter from the current input
                newParameter = new SqlParameter(currentParameter.Item1, currentParameter.Item2);
                newParameter.Value = currentParameter.Item3;

                // Appending the parameter to the query
                queryAdapter.SelectCommand.Parameters.Add(newParameter);
            }

            response = new DataTable();

            try
            {
                // The actual DB query
                queryAdapter.Fill(response);
            }
            catch (Exception ex)
            {
                throw new Exception("Could not retrieve the data with the command: " + selectCommand, ex);
            }

            return response;
        }
    }
}
