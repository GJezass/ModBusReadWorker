using System.Net.Sockets;
using System.Security;
using Modbus.Device;

namespace BBox_ModBusWorker
{
    #region Simular inser��o

    /// <summary>
    /// Class para simula��o de inser��o de dados
    /// </summary>
    public static class EquipmentSim
    {

        /// <summary>
        /// M�todo de inser��o de dados no ficheiro de base de dados
        /// </summary>
        /// <param name="connString">Passa a connection string</param>
        public static void InsertValues(string connString)
        {
            // DatabaseHelper.InitializeDatabase(connString);

            // Insert DataSource
            DatabaseHelper.InsertDataSource(new DataSource
            {
                Name = "SCR_Automation",
                IPAddress = "127.0.0.1",
                Port = 502

            }, connString);

            // Insert Equipamento
            DatabaseHelper.InsertEquipment(new Equipment
            {
                Name = "UREA",
                BoemName = "UREA",
                DataSourceId = 1

            }, connString);

            // Insert Vari�vel
            DatabaseHelper.InsertVariable(new Variable
            {
                Name = "Temp",
                BoemName = "temp",
                StartAddress = 10,
                NumRegisters = 2,
                EquipmentId = 1

            }, connString);

        }


    }

    #endregion

    /// <summary>
    /// Classe do servi�o
    /// </summary>
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly string? _connString;
        private readonly string? _clientId;
        private readonly string? _shipId;
        private readonly bool _dbSource;

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            
            _connString = _configuration.GetConnectionString("SQLite");
            _clientId = _configuration.GetValue<string>("ShipSettings:ClientID", defaultValue: "");
            _shipId = _configuration.GetValue<string>("ShipSettings:ShipID", defaultValue: "");
            _dbSource = _configuration.GetValue<bool>("ShipSettings:DBsource");

        }

        /// <summary>
        /// Main task
        /// </summary>
        /// <param name="stoppingToken">passa flag de cancelamento</param>
        /// <returns></returns>
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            // inicia BD
            if (_dbSource) await DatabaseHelper.InitializeDatabase(_connString);
            //EquipmentSim.InsertValues(connString);

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("\n********* ModBus Worker running at: {time} ***********", DateTime.Now);

                // lista de DataSources
                List<DataSource> data_list;
                // leitura por base de dados ou config
                if (!_dbSource) data_list = GetDataSourcesConfig();
                else data_list = DatabaseHelper.GetDataSources(_connString);
                

                foreach(var source in data_list)
                {                  
                    try
                    {
                        _logger.LogInformation($"\nConnecting to {source.Name} at {source.IPAddress}:{source.Port}");

                        /// para cada Datasource, lista equipamentos
                        /// passa a connection string e id da atual DataSource
                        List<Equipment> equi_list;
                        // leitura por base de dados ou config
                        if(!_dbSource) equi_list = GetEquipmentsConfig(source.Id);
                        else equi_list = DatabaseHelper.GetEquipments(_connString, source.Id);


                        foreach (var equi in equi_list)
                        {
                            /// para cada equipamento, lista vari�veis
                            /// passa a connection string e id do equipamento atual
                            List<Variable> vars_list;
                            // leitura por base de dados ou config
                            if (!_dbSource) vars_list = GetVariablesConfig(equi.Id);
                            else vars_list = DatabaseHelper.GetVariables(_connString, equi.Id);

                            /// lista tasks para cada vari�vel
                            /// chama m�todo de leitura para cada uma
                            var readingTasks = vars_list.Select(varE => ReadVariableAsync(source, equi.Name, varE)).ToList();
                            await Task.WhenAll(readingTasks);
                        }

                        _logger.LogInformation("Data reading completed successfully.");
                    }
                    catch (Exception plcErr)
                    {
                        _logger.LogError($"Error communicating with {source.Name}: {plcErr.Message}");
                    }               
                }               
                await Task.Delay(_configuration.GetValue<int>("ShipSettings:ReadingInterval"), stoppingToken);
            }
                         
        }

        #region M�todos privados

        /// <summary>
        /// M�todo de leitura de vari�veis
        /// </summary>
        /// <param name="src">passa o DataSource <see cref="DataSource"/></param>
        /// <param name="equiName">passa o nome do equipamento <see cref="Equipment"/></param>
        /// <param name="varE">passa a vari�vel <see cref="Variable"/></param>
        /// <returns></returns>
        private async Task ReadVariableAsync(DataSource src, string equiName, Variable varE)
        {
            try
            {
                // inicia liga��o TCP e cria o master ModBus com IP e o porto da source
                using (TcpClient client = new TcpClient(src.IPAddress, src.Port))
                using (ModbusIpMaster master = ModbusIpMaster.CreateIp(client))
                {
                    // Listagem dos registos com o endere�o inicial da vari�vel e n�mero de registos a devolver
                    ushort[] registers = await master.ReadHoldingRegistersAsync(1, varE.StartAddress, varE.NumRegisters);
                    /// chamada do m�todo de registo de valores
                    /// passa o nome do equipamento, da vari�vel e lista de registos (16 bits x1)
                    LogValues(equiName, varE.Name, registers);

                }
            }
            catch (Exception varErr)
            {
                _logger.LogError($"Error reading variable {equiName} - {varE.Name}: {varErr.Message}");
            }
        }

        /// <summary>
        /// M�todo de registo de valores em consola e em ficheiros
        /// </summary>
        /// <param name="setName">Nome do equipamento</param>
        /// <param name="varName">Nome da vari�vel</param>
        /// <param name="registers">Lista registos a ler</param>
        private void LogValues(string setName, string varName, ushort[] registers)
        {
            // vari�veis para a formata��o da data nos ficheiros
            var currentDate = DateTime.Now; // data atual
            var month = currentDate.Month.ToString().PadLeft(2, '0'); // n�mero m�s ex.: 02 (fevereiro)
            var day = currentDate.Day.ToString().PadLeft(3, '0'); // n�mero dia ex.: 024 

            // formata��o do nome e path do ficheiro a criar
            var directoryPath = Path.Combine(Environment.CurrentDirectory, $"vars/{currentDate.Year}/{month}/{day}/{setName}/{varName}");
            var fileName = $"{_clientId}_{_shipId}-{setName}_{varName}_{day}.csv";
            var filePath = Path.Combine(directoryPath, fileName);

            // cria a pasta se n�o existir
            if (!Directory.Exists(directoryPath))
            {
                Directory.CreateDirectory(directoryPath);
            }

            // L� cada registo ushort (16 bits), agrupando de 2 em 2
            for (int i = 0; i < registers.Length; i += 2)
            {
                // se n�o passa o limite
                if (i + 1 < registers.Length)
                {
                    // convers�o para bytes com a concatena��o do registo atual com o pr�ximo (16 bits + 16 bits = 4 bytes)
                    byte[] bytes = BitConverter.GetBytes(registers[i]).Concat(BitConverter.GetBytes(registers[i + 1])).ToArray();

                    //if (BitConverter.IsLittleEndian) Array.Reverse(bytes);

                    // Se existerem 4 bytes / 32 bits
                    if (bytes.Length == 4)
                    {
                        float temp = BitConverter.ToSingle(bytes, 0); // converte para float
                        _logger.LogInformation($"Received {setName} - {varName} as float: {temp.ToString("F3")}");

                        // abre o ficheiro e escreve (incremental)
                        using (StreamWriter sw = new StreamWriter(filePath, true))
                        {
                            sw.WriteLine($"{currentDate:yyyy-MM-dd HH:mm:ss},,{temp.ToString("F3")}");
                        }
                    }
                    else
                    {
                        _logger.LogError($"Invalid byte array length for float conversion.");
                    }
                }
                else
                {
                    _logger.LogError($"Insufficient data to form a pair of ushort values.");
                }
            }


            _logger.LogInformation($"\n");

        }

        /// <summary>
        /// Faz a leitura de Datasources registadas no ficheiro de configura��o 
        /// </summary>
        /// <returns>Devolve uma lista de Datasources <see cref="DataSource"/></returns>
        private List<DataSource> GetDataSourcesConfig()
        {
            // A cada datasource associa � classe
            return _configuration.GetSection("EquipmentSettings:DataSources")
                .GetChildren()
                .Select(ds => new DataSource
                {
                    Id = int.Parse(ds["Id"]),
                    Name = ds["Name"],
                    IPAddress = ds["IPAddress"],
                    Port = int.Parse(ds["Port"]),
                })
                .ToList();
        }
        /// <summary>
        /// Faz a leitura de Equipamentos registados no ficheiro de configura��o
        /// </summary>
        /// <param name="datasourceId">passa o id da datasource</param>
        /// <returns>Devolve lista de Equipamentos <see cref="Equipment"/></returns>
        private List<Equipment> GetEquipmentsConfig(int datasourceId)
        {
            // a cada equipamento ligado � datasource, associa � classe
            return _configuration.GetSection("EquipmentSettings:Equipamentos")
                .GetChildren()
                .Select(eq => new Equipment
                {
                    Id = int.Parse(eq["Id"]),
                    Name = eq["Name"],
                    BoemName = eq["BoemName"],
                    DataSourceId = datasourceId,
                })
                .ToList();
        }
        /// <summary>
        /// Faz a leitura de vari�veis registadas no ficheiro de configura��o
        /// </summary>
        /// <param name="equipmentId">passa o id do equipamento</param>
        /// <returns>Devolve lista de vari�veis <see cref="Variable"/></returns>
        private List<Variable> GetVariablesConfig(int equipmentId)
        {
            // a cada vari�vel ligado ao equipamento, associa � classe
            return _configuration.GetSection("EquipmentSettings:Variaveis")
                .GetChildren()
                .Select(varE => new Variable
                {
                    Id = int.Parse(varE["Id"]),
                    Name = varE["Name"],
                    BoemName = varE["BoemName"],
                    StartAddress = ushort.Parse(varE["StartAddress"]),
                    NumRegisters = ushort.Parse(varE["NumRegisters"]),
                    EquipmentId = equipmentId,
                })
                .ToList();
        }

        #endregion


    }
}