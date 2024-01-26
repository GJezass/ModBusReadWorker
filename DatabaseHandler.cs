using System.Data.SqlClient;
using System.Data.SQLite;
using System.Net;

namespace BBox_ModBusWorker
{
    #region Modelos BD

    /// <summary>
    /// DataSource Model
    /// </summary>
    public class DataSource
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string IPAddress { get; set; }
        public int Port { get; set; }
    }

    /// <summary>
    /// Equipment Model
    /// </summary>
    public class Equipment
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string BoemName { get; set; }
        public int DataSourceId { get; set; } // FK DataSource

    }

    /// <summary>
    /// Variable Model
    /// </summary>
    public class Variable
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string BoemName { get; set; }
        public ushort StartAddress { get; set; }
        public ushort NumRegisters { get; set; }
        public int EquipmentId { get; set; } // FK Equipment

    }


    #endregion
     
    /// <summary>
    /// Classe para vários métodos com a BD
    /// </summary>
    public static class DatabaseHelper
    {

        #region Inicia BD e criação de tabelas

        /// <summary>
        /// Inicia uma ligação com a bd e cria tabelas caso estas não existam
        /// </summary>
        /// <param name="connectionString">passa a connection string</param>
        public static async Task InitializeDatabase(string connectionString)
        {
            // abre ligação
            using (SQLiteConnection connection = new SQLiteConnection(connectionString))
            {
                await connection.OpenAsync();

                using (SQLiteCommand command = connection.CreateCommand())
                {
                    // cria tabela DataSource
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS Data_Sources(
                            Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                            Name TEXT NOT NULL,
                            IP_address TEXT NOT NULL,
                            Port INTEGER NOT NULL
                        );"
                    ;

                    await command.ExecuteNonQueryAsync();

                    // cria tabela Equipamentos
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS Equipments(
                            Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                            Name TEXT NOT NULL,
                            BOEM_name TEXT,
                            DataSource_Id INTEGER,
                            FOREIGN KEY (DataSource_Id) REFERENCES Data_Sources (Id)
                        );"
                    ;

                    await command.ExecuteNonQueryAsync();

                    // cria tabela Variaveis
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS Variables(
                            Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                            Name TEXT NOT NULL,
                            BOEM_name TEXT NOT NULL,
                            Start_address INTEGER NOT NULL,
                            Num_registers INTEGER NOT NULL,
                            Equipment_Id INTEGER,
                            FOREIGN KEY (Equipment_Id) REFERENCES Equipments (Id)
                        );"
                    ;

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        #endregion

        #region Inserts
        ////////////////// operações Insert ////////////////

        /// <summary>
        /// Insere em base de dados um novo registo para uma DataSource
        /// </summary>
        /// <param name="dataSource">objecto DataSource<see cref="DataSource"/></param>
        /// <param name="connString">passa a connection string</param>
        public static void InsertDataSource(DataSource dataSource, string connString)
        {
            using (SQLiteConnection connection = new SQLiteConnection(connString))
            {
                connection.Open();
                using (SQLiteCommand command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO Data_Sources (Name, IP_address, Port)
                        VALUES (@Name, @IPaddress, @Port);
                    ";

                    command.Parameters.AddWithValue("@Name", dataSource.Name);
                    command.Parameters.AddWithValue("@IPaddress", dataSource.IPAddress);
                    command.Parameters.AddWithValue("@Port", dataSource.Port);

                    command.ExecuteNonQuery();

                }
            }
        }

        /// <summary>
        /// Insere em base de dados um novo registo para um Equipamento
        /// </summary>
        /// <param name="equipment">objecto Equipment<see cref="Equipment"/></param>
        /// <param name="connString">passa a connection string</param>
        public static void InsertEquipment(Equipment equipment, string connString)
        {
            using (SQLiteConnection connection = new SQLiteConnection(connString))
            {
                connection.Open();
                using (SQLiteCommand command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO Equipments (Name, BOEM_name, DataSource_Id)
                        VALUES (@Name, @BoemName, @DataSourceId);
                    ";

                    command.Parameters.AddWithValue("@Name", equipment.Name);
                    command.Parameters.AddWithValue("@BoemName", equipment.BoemName);
                    command.Parameters.AddWithValue("@DataSourceId", equipment.DataSourceId);

                    command.ExecuteNonQuery();

                }
            }
        }

        /// <summary>
        /// Insere em base de dados um novo registo para uma variável
        /// </summary>
        /// <param name="variable">objecto Variable<see cref="Variable"/></param>
        /// <param name="connString">passa a connection string</param>
        public static void InsertVariable(Variable variable, string connString)
        {
            using (SQLiteConnection connection = new SQLiteConnection(connString))
            {
                connection.Open();
                using (SQLiteCommand command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO Variables (Name, Boem_name, Start_address, Num_registers, Equipment_Id)
                        VALUES (@Name, @BoemName, @StartAddress, @NumRegisters, @EquipmentId)
                    ";

                    command.Parameters.AddWithValue("@Name", variable.Name);
                    command.Parameters.AddWithValue("@BoemName", variable.BoemName);
                    command.Parameters.AddWithValue("@StartAddress", variable.StartAddress);
                    command.Parameters.AddWithValue("@NumRegisters", variable.NumRegisters);
                    command.Parameters.AddWithValue("@EquipmentId", variable.EquipmentId);

                    command.ExecuteNonQuery();
                }
            }
        }
        #endregion

        #region Selects

        ////////////////// Operações Get ////////////////

        /// <summary>
        /// Consulta à bd para listagem de DataSources
        /// </summary>
        /// <param name="connString">passa a connection string</param>
        /// <returns>Devolve uma lista das Datasources em bd</returns>
        public static List<DataSource> GetDataSources(string connString)
        {
            List<DataSource> fontes_lista = new List<DataSource>();

            using (SQLiteConnection connection = new SQLiteConnection(connString))
            {
                connection.Open();
                using (SQLiteCommand command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT * FROM Data_Sources;";
                    using (SQLiteDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            fontes_lista.Add(new DataSource
                            {
                                Id = Convert.ToInt32(reader["Id"]),
                                Name = reader["Name"].ToString(),
                                IPAddress = reader["IP_address"].ToString(),
                                Port = Convert.ToInt32(reader["Port"])
                            });
                        }
                    }

                }
            }

            return fontes_lista;
        }

        /// <summary>
        /// Consulta à bd para listagem de Equipamentos
        /// </summary>
        /// <param name="connString">passa a connection string</param>
        /// <param name="DataSource_Id">passa id do datasource</param>
        /// <returns>Devolve uma lista do/s Equipamentos em bd com o respetivo datasource id</returns>
        public static List<Equipment> GetEquipments(string connString, int DataSource_Id)
        {
            List<Equipment> eqipamentos_lista = new List<Equipment>();
            using (SQLiteConnection connection = new SQLiteConnection(connString))
            {
                connection.Open();
                using (SQLiteCommand command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT * FROM Equipments WHERE DataSource_Id = {DataSource_Id};";
                    using (SQLiteDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            eqipamentos_lista.Add(new Equipment
                            {
                                Id = Convert.ToInt32(reader["Id"]),
                                Name = reader["Name"].ToString(),
                                BoemName = reader["BOEM_name"].ToString(),
                                DataSourceId = Convert.ToInt32(reader["DataSource_Id"])

                            });
                        }
                    }
                }
            }
            return eqipamentos_lista;
        }

        /// <summary>
        /// Consulta à bd para listagem de variáveis
        /// </summary>
        /// <param name="connString">passa a connection string</param>
        /// <param name="Equipment_Id">passa id do equipamento</param>
        /// <returns>Devolve uma lista do/s Equipamentos em bd com o respetivo datasource id</returns>
        public static List<Variable> GetVariables(string connString, int Equipment_Id)
        {
            List<Variable> vars_lista = new List<Variable>();
            using (SQLiteConnection connection = new SQLiteConnection(connString))
            {
                connection.Open();
                using (SQLiteCommand command = connection.CreateCommand())
                {
                    command.CommandText = $"SELECT * FROM Variables WHERE Equipment_Id = {Equipment_Id};";
                    using (SQLiteDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            vars_lista.Add(new Variable
                            {
                                Id = Convert.ToInt32(reader["Id"]),
                                Name = reader["Name"].ToString(),
                                BoemName = reader["BOEM_name"].ToString(),
                                StartAddress = (ushort)Convert.ToInt16(reader["Start_address"]),
                                NumRegisters = (ushort)Convert.ToInt16(reader["Num_registers"]),
                                EquipmentId = Convert.ToInt32(reader["Equipment_Id"])
                            });
                        }
                    }
                }
            }
            return vars_lista;
        }

        #endregion

    }
}

        
    
