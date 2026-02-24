const { ref, reactive, computed, onMounted } = Vue;
const { ElMessage, ElMessageBox } = ElementPlus;

export default {
    setup() {
        const jobConfig = reactive({
            jobName: '',
            parallelism: 1,
            checkpointInterval: 30000
        });

        const sourceTables = ref([]);
        const sinkTables = ref([]);
        const queries = ref([]);
        const datasources = ref([]);
        const submitting = ref(false);
        const sqlPreviewVisible = ref(false);
        const generatedSql = ref('');
        const tableSelectionVisible = ref(false);
        const currentTable = ref(null);
        const currentTableType = ref('source');
        const currentSourceTabIndex = ref('0');
        const currentSinkTabIndex = ref('0');
        const tableList = ref([]);
        const loadingTables = ref(false);
        const selectedTables = ref([]);
        const batchSyncEnabled = ref(false);
        const createTableSqlVisible = ref(false);
        const currentCreateTableSql = ref('');
        const currentEditingTable = ref(null);

        const sourceConnectorGroups = ref([
            {
                label: 'CDC 连接器',
                options: [
                    { value: 'mysql-cdc', label: 'MySQL CDC' },
                    { value: 'postgres-cdc', label: 'PostgreSQL CDC' },
                    { value: 'sqlserver-cdc', label: 'SQL Server CDC' },
                    { value: 'oracle-cdc', label: 'Oracle CDC' },
                    { value: 'tidb-cdc', label: 'TiDB CDC' },
                    { value: 'oceanbase-cdc', label: 'OceanBase CDC' }
                ]
            },
            {
                label: '批处理/其他',
                options: [
                    { value: 'jdbc', label: 'JDBC' },
                    { value: 'kafka', label: 'Kafka' },
                    { value: 'doris', label: 'Doris' },
                    { value: 'starrocks', label: 'StarRocks' },
                    { value: 'redis', label: 'Redis' },
                    { value: 'filesystem', label: 'FileSystem' },
                    { value: 'socket', label: 'Socket' },
                    { value: 'datagen', label: 'DataGen' }
                ]
            }
        ]);

        const sinkConnectorGroups = ref([
            {
                label: '消息队列',
                options: [
                    { value: 'kafka', label: 'Kafka' },
                    { value: 'pulsar', label: 'Pulsar' },
                    { value: 'rabbitmq', label: 'RabbitMQ' }
                ]
            },
            {
                label: '数据仓库',
                options: [
                    { value: 'starrocks', label: 'StarRocks' },
                    { value: 'clickhouse', label: 'ClickHouse' },
                    { value: 'doris', label: 'Doris' }
                ]
            },
            {
                label: '数据库 JDBC',
                options: [
                    { value: 'jdbc', label: 'MySQL JDBC' },
                    { value: 'postgres-jdbc', label: 'PostgreSQL JDBC' },
                    { value: 'oracle-jdbc', label: 'Oracle JDBC' },
                    { value: 'sqlserver-jdbc', label: 'SQL Server JDBC' },
                    { value: 'jdbc-upsert', label: 'JDBC Upsert' }
                ]
            },
            {
                label: '数据存储',
                options: [
                    { value: 'filesystem', label: 'FileSystem' },
                    { value: 'hbase', label: 'HBase' },
                    { value: 'elasticsearch', label: 'Elasticsearch' }
                ]
            },
            {
                label: '测试工具',
                options: [
                    { value: 'print', label: 'Print' },
                    { value: 'blackhole', label: 'Blackhole' }
                ]
            }
        ]);

        const sinkCommonConfig = ref({
            connectorType: 'doris',
            datasourceId: null,
            jdbcUrl: '',
            jdbcDriver: 'com.mysql.cj.jdbc.Driver',
            jdbcUsername: '',
            jdbcPassword: '',
            dorisUrl: '',
            dorisDriver: 'com.mysql.cj.jdbc.Driver',
            dorisUsername: '',
            dorisPassword: '',
            starrocksUrl: '',
            starrocksDriver: 'com.mysql.cj.jdbc.Driver',
            starrocksUsername: '',
            starrocksPassword: '',
            clickhouseUrl: '',
            clickhouseDriver: 'com.clickhouse.jdbc.ClickHouseDriver',
            clickhouseUsername: '',
            clickhousePassword: '',
            kafkaBootstrapServers: 'localhost:9092',
            kafkaFormat: 'json',
            kafkaSemantic: 'at-least-once',
            autoCreateTable: true,
            tableType: 'UNIQUE',
            replicationNum: 1,
            dorisBuckets: 10,
            enableBatch: false,
            batchSize: 5000,
            batchInterval: 1000
        });

        const sourceCommonConfig = ref({
            connectorType: 'mysql-cdc',
            datasourceId: null,
            host: 'localhost',
            port: 3306,
            database: '',
            timezone: 'Asia/Shanghai',
            username: '',
            password: '',
            scanMode: 'initial',
            incrementalSnapshot: true,
            postgresSchema: 'public',
            postgresSlotName: 'flink_slot',
            sqlserverSchema: 'dbo',
            tidbPdAddresses: 'localhost:2379',
            oceanbaseTenant: 'sys',
            kafkaBootstrapServers: 'localhost:9092',
            kafkaGroup: 'flink-consumer',
            kafkaStartMode: 'latest',
            kafkaFormat: 'json'
        });

        const queryTypes = ref([
            { value: 'insert', label: 'INSERT 插入' },
            { value: 'select', label: 'SELECT 查询' },
            { value: 'aggregate', label: '聚合查询' },
            { value: 'join', label: 'JOIN 关联' },
            { value: 'window', label: '窗口聚合' },
            { value: 'custom', label: '自定义SQL' }
        ]);

        const formatTypes = ref([
            { value: 'json', label: 'JSON' },
            { value: 'csv', label: 'CSV' },
            { value: 'avro', label: 'Avro' },
            { value: 'debezium-json', label: 'Debezium JSON' },
            { value: 'canal-json', label: 'Canal JSON' },
            { value: 'maxwell-json', label: 'Maxwell JSON' },
            { value: 'protobuf', label: 'Protobuf' },
            { value: 'raw', label: 'Raw' }
        ]);

        const fieldTypes = [
            'STRING', 'VARCHAR', 'CHAR', 'BOOLEAN', 'TINYINT', 'SMALLINT',
            'INT', 'INTEGER', 'BIGINT', 'FLOAT', 'DOUBLE', 'DECIMAL',
            'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'ARRAY', 'MAP',
            'ROW', 'MULTISET', 'BINARY', 'VARBINARY'
        ];

        function addSourceTable() {
            const newTable = {
                tableName: '',
                datasourceId: null,
                connectorType: 'mysql-cdc',
                format: 'json',
                fields: [],
                enableWatermark: false,
                watermarkField: '',
                watermarkDelay: 5,
                kafkaTopic: '',
                kafkaBootstrapServers: 'localhost:9092',
                kafkaGroup: 'flink-consumer',
                kafkaStartMode: 'latest',
                mysqlStartMode: 'initial',
                jdbcHost: 'localhost',
                jdbcPort: 3306,
                jdbcDatabase: '',
                jdbcTable: '',
                jdbcUsername: '',
                jdbcPassword: '',
                jdbcDriver: 'com.mysql.cj.jdbc.Driver',
                jdbcUrl: '',
                mysqlHost: 'localhost:3306',
                mysqlDatabase: '',
                mysqlTable: '',
                mysqlUsername: '',
                mysqlPassword: '',
                mysqlServerTimezone: 'Asia/Shanghai',
                cdcScanMode: 'initial',
                debeziumLockingMode: 'none',
                cdcIncrementalSnapshot: true,
                oracleHost: 'localhost:1521',
                oracleServiceName: 'ORCL',
                oracleTable: '',
                oracleUsername: '',
                oraclePassword: '',
                sqlserverHost: 'localhost:1433',
                sqlserverDatabase: '',
                sqlserverTable: '',
                sqlserverSchema: 'dbo',
                sqlserverUsername: '',
                sqlserverPassword: '',
                postgresHost: 'localhost:5432',
                postgresDatabase: '',
                postgresTable: '',
                postgresSchema: 'public',
                postgresUsername: '',
                postgresPassword: '',
                postgresSlotName: 'flink_slot',
                tidbHost: 'localhost:4000',
                tidbDatabase: '',
                tidbTable: '',
                tidbUsername: '',
                tidbPassword: '',
                tidbPdAddresses: 'localhost:2379',
                mongodbHost: 'mongodb://localhost:27017',
                mongodbDatabase: '',
                mongodbCollection: '',
                mongodbUsername: '',
                mongodbPassword: '',
                oceanbaseHost: 'localhost:2881',
                oceanbaseTenant: 'sys',
                oceanbaseDatabase: '',
                oceanbaseTable: '',
                oceanbaseUsername: '',
                oceanbasePassword: '',
                oceanbaseSchema: '',
                dorisHost: 'localhost:8030',
                dorisDatabase: '',
                dorisTable: '',
                dorisUsername: '',
                dorisPassword: '',
                starrocksHost: 'localhost:9030',
                starrocksDatabase: '',
                starrocksTable: '',
                starrocksUsername: '',
                starrocksPassword: '',
                redisHost: 'localhost:6379',
                redisPassword: '',
                redisDatabase: 0,
                redisDataType: 'string',
                redisKeyPrefix: '',
                redisTTL: 0,
                filePath: '',
                socketHost: 'localhost',
                socketPort: 9999,
                rowsPerSecond: 1,
                numberOfRows: null
            };
            
            autoAddFields(newTable, 'source');
            sourceTables.value.push(newTable);
        }

        function autoAddFields(table, type) {
            if (table.fields.length === 0) {
                table.fields = [
                    { name: 'id', type: 'BIGINT', precision: '', scale: '', primaryKey: true },
                    { name: 'name', type: 'STRING', precision: '200', scale: '', primaryKey: false }
                ];
            }
        }

        function removeSourceTable(index) {
            sourceTables.value.splice(index, 1);
            if (sourceTables.value.length === 0) {
                currentSourceTabIndex.value = '0';
            } else if (parseInt(currentSourceTabIndex.value) >= sourceTables.value.length) {
                currentSourceTabIndex.value = String(sourceTables.value.length - 1);
            }
        }

        function createSinkTableFromSource(sourceTable, sourceFlinkTableName) {
            return {
                tableName: sourceFlinkTableName + '_sink',
                physicalTableName: sourceFlinkTableName + '_sink',
                autoCreateTable: true,
                connectorType: sinkCommonConfig.value.connectorType,
                format: 'json',
                fields: JSON.parse(JSON.stringify(sourceTable.fields || [])),
                datasourceId: sinkCommonConfig.value.datasourceId,
                kafkaTopic: '',
                kafkaBootstrapServers: sinkCommonConfig.value.kafkaBootstrapServers,
                jdbcUrl: sinkCommonConfig.value.jdbcUrl,
                jdbcDriver: sinkCommonConfig.value.jdbcDriver,
                jdbcTable: '',
                jdbcUsername: sinkCommonConfig.value.jdbcUsername,
                jdbcPassword: sinkCommonConfig.value.jdbcPassword,
                starrocksUrl: sinkCommonConfig.value.starrocksUrl,
                starrocksTable: '',
                starrocksUsername: sinkCommonConfig.value.starrocksUsername,
                starrocksPassword: sinkCommonConfig.value.starrocksPassword,
                starrocksDriver: sinkCommonConfig.value.starrocksDriver,
                starrocksWriteMode: 'append',
                starrocksFeHost: '',
                starrocksFePort: 8030,
                clickhouseUrl: sinkCommonConfig.value.clickhouseUrl,
                clickhouseTable: '',
                clickhouseUsername: sinkCommonConfig.value.clickhouseUsername,
                clickhousePassword: sinkCommonConfig.value.clickhousePassword,
                clickhouseDriver: sinkCommonConfig.value.clickhouseDriver,
                clickhouseWriteMode: 'insert',
                dorisUrl: sinkCommonConfig.value.dorisUrl,
                dorisTable: '',
                dorisUsername: sinkCommonConfig.value.dorisUsername,
                dorisPassword: sinkCommonConfig.value.dorisPassword,
                dorisDriver: sinkCommonConfig.value.dorisDriver,
                dorisFeHost: '',
                dorisFePort: 8030,
                replicationNum: 1,
                dorisBuckets: 10,
                tableType: 'UNIQUE',
                enableBatchWrite: false,
                batchSize: 10000,
                flushInterval: 30000,
                maxRetries: 3,
                retryInterval: 10000,
                writeMode: 'stream_load',
                filePath: '',
                sourceTableName: sourceFlinkTableName
            };
        }

        function addSinkTable() {
            sinkTables.value.push({
                tableName: '',
                physicalTableName: '',
                autoCreateTable: true,
                connectorType: sinkCommonConfig.value.connectorType,
                format: 'json',
                fields: [],
                datasourceId: sinkCommonConfig.value.datasourceId,
                kafkaTopic: '',
                kafkaBootstrapServers: sinkCommonConfig.value.kafkaBootstrapServers,
                jdbcUrl: sinkCommonConfig.value.jdbcUrl,
                jdbcDriver: sinkCommonConfig.value.jdbcDriver,
                jdbcTable: '',
                jdbcUsername: sinkCommonConfig.value.jdbcUsername,
                jdbcPassword: sinkCommonConfig.value.jdbcPassword,
                starrocksUrl: sinkCommonConfig.value.starrocksUrl,
                starrocksTable: '',
                starrocksUsername: sinkCommonConfig.value.starrocksUsername,
                starrocksPassword: sinkCommonConfig.value.starrocksPassword,
                starrocksDriver: sinkCommonConfig.value.starrocksDriver,
                starrocksWriteMode: 'append',
                starrocksFeHost: '',
                starrocksFePort: 8030,
                clickhouseUrl: sinkCommonConfig.value.clickhouseUrl,
                clickhouseTable: '',
                clickhouseUsername: sinkCommonConfig.value.clickhouseUsername,
                clickhousePassword: sinkCommonConfig.value.clickhousePassword,
                clickhouseDriver: sinkCommonConfig.value.clickhouseDriver,
                clickhouseWriteMode: 'insert',
                dorisUrl: sinkCommonConfig.value.dorisUrl,
                dorisTable: '',
                dorisUsername: sinkCommonConfig.value.dorisUsername,
                dorisPassword: sinkCommonConfig.value.dorisPassword,
                dorisDriver: sinkCommonConfig.value.dorisDriver,
                dorisFeHost: '',
                dorisFePort: 8030,
                replicationNum: 1,
                dorisBuckets: 10,
                tableType: 'UNIQUE',
                enableBatchWrite: false,
                batchSize: 10000,
                flushInterval: 30000,
                maxRetries: 3,
                retryInterval: 10000,
                writeMode: 'stream_load',
                filePath: ''
            });
        }

        function removeSinkTable(index) {
            sinkTables.value.splice(index, 1);
            if (sinkTables.value.length === 0) {
                currentSinkTabIndex.value = '0';
            } else if (parseInt(currentSinkTabIndex.value) >= sinkTables.value.length) {
                currentSinkTabIndex.value = String(sinkTables.value.length - 1);
            }
        }

        function addField(table) {
            table.fields.push({
                name: '',
                type: 'STRING',
                precision: '',
                scale: '',
                primaryKey: false
            });
        }

        function removeField(table, index) {
            table.fields.splice(index, 1);
        }

        function handlePrimaryKey(table, index) {
            const field = table.fields[index];
            if (field.primaryKey) {
                table.fields.forEach((f, i) => {
                    if (i !== index) f.primaryKey = false;
                });
            }
        }

        function addQuery() {
            const sourceTable = sourceTables.value.length > 0 ? sourceTables.value[0].tableName : '';
            const sinkTable = sinkTables.value.length > 0 ? sinkTables.value[0].tableName : '';
            const sourceTableFields = getFieldsByTable(sourceTable);

            const newQuery = {
                sinkTable: sinkTable,
                sourceTable: sourceTable,
                queryType: 'insert',
                selectFields: [],
                selectedFields: [],
                groupByFields: [],
                aggregates: [],
                whereClause: '',
                havingClause: '',
                joinType: 'INNER',
                joinTable: '',
                joinCondition: '',
                windowType: 'TUMBLE',
                windowSize: '5 MINUTES',
                slideSize: '1 MINUTE',
                windowTimeField: '',
                customSql: ''
            };
            queries.value.push(newQuery);

            if (sourceTableFields.length > 0) {
                const fieldsToSelect = sourceTableFields.slice(0, 5).map(f => f.name);
                newQuery.selectFields = fieldsToSelect;
                newQuery.selectedFields = fieldsToSelect;
            }
        }

        function removeQuery(index) {
            queries.value.splice(index, 1);
        }

        function clearAllQueries() {
            queries.value = [];
        }

        function addAggregate(query) {
            query.aggregates = query.aggregates || [];
            query.aggregates.push({ function: 'COUNT', field: '', alias: '' });
        }

        function removeAggregate(query, index) {
            query.aggregates.splice(index, 1);
        }

        function getFieldsByTable(tableName) {
            const sourceTable = sourceTables.value.find(t => t.tableName === tableName);
            return sourceTable ? (sourceTable.fields || []) : [];
        }

        function getTimeFields(fields) {
            if (!fields || !Array.isArray(fields)) return [];
            return fields.filter(f => 
                f.type === 'TIMESTAMP' || f.type === 'TIMESTAMP_LTZ' || f.type === 'DATE' || f.type === 'BIGINT'
            );
        }

        function getOtherSourceTables(currentTableName) {
            return sourceTables.value.filter(t => t.tableName !== currentTableName);
        }

        function getConnectorLabel(type) {
            const allOptions = [...sourceConnectorGroups.value, ...sinkConnectorGroups.value].flatMap(g => g.options);
            const option = allOptions.find(o => o.value === type);
            return option ? option.label : type;
        }

        function syncTableName(table, type) {
            if (type === 'main') {
                if (table.connectorType === 'doris' || table.connectorType === 'doris-cdc') {
                    table.dorisTable = table.tableName;
                } else if (table.connectorType === 'starrocks' || table.connectorType === 'starrocks-cdc') {
                    table.starrocksTable = table.tableName;
                } else if (table.connectorType === 'mongodb-cdc') {
                    table.mongodbCollection = table.tableName;
                } else if (table.connectorType === 'oceanbase-cdc') {
                    table.oceanbaseTable = table.tableName;
                }
            }
        }

        async function applyDatasource(table, datasourceId) {
            if (!datasourceId) return;
            const ds = datasources.value.find(d => d.id === datasourceId);
            if (!ds) return;

            table.datasourceId = datasourceId;

            if (table.connectorType === 'jdbc' || table.connectorType === 'jdbc-upsert') {
                table.jdbcHost = ds.host;
                table.jdbcPort = ds.port;
                table.jdbcDatabase = ds.database;
                table.jdbcUsername = ds.username;
                table.jdbcPassword = ds.password;
                
                if (!table.tableName) {
                    table.tableName = ds.name + '_sink';
                }
                table.jdbcTable = table.tableName;
                
                if (ds.type === 'mysql') {
                    table.jdbcUrl = 'jdbc:mysql://' + ds.host + ':' + ds.port + '/' + ds.database;
                    table.jdbcDriver = 'com.mysql.cj.jdbc.Driver';
                } else if (ds.type === 'postgresql') {
                    table.jdbcUrl = 'jdbc:postgresql://' + ds.host + ':' + ds.port + '/' + ds.database;
                    table.jdbcDriver = 'org.postgresql.Driver';
                } else if (ds.type === 'oracle') {
                    table.jdbcUrl = 'jdbc:oracle:thin:@' + ds.host + ':' + ds.port + ':' + ds.database;
                    table.jdbcDriver = 'oracle.jdbc.OracleDriver';
                } else if (ds.type === 'sqlserver') {
                    table.jdbcUrl = 'jdbc:sqlserver://' + ds.host + ':' + ds.port + ';databaseName=' + ds.database;
                    table.jdbcDriver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver';
                } else if (ds.type === 'clickhouse') {
                    table.jdbcUrl = 'jdbc:clickhouse://' + ds.host + ':' + ds.port + '/' + ds.database;
                    table.jdbcDriver = 'com.clickhouse.jdbc.ClickHouseDriver';
                } else {
                    table.jdbcUrl = 'jdbc:mysql://' + ds.host + ':' + ds.port + '/' + ds.database;
                    table.jdbcDriver = 'com.mysql.cj.jdbc.Driver';
                }
            } else if (table.connectorType === 'mysql-cdc') {
                table.mysqlHost = ds.host + ':' + ds.port;
                table.mysqlUsername = ds.username;
                table.mysqlPassword = ds.password;
                table.mysqlDatabase = ds.database;
                table.jdbcHost = ds.host;
                table.jdbcPort = ds.port;
                table.jdbcDatabase = ds.database;
                table.jdbcUsername = ds.username;
                table.jdbcPassword = ds.password;
                
                if (!table.tableName) {
                    table.tableName = ds.name + '_source';
                }
            } else if (table.connectorType === 'doris' || table.connectorType === 'doris-cdc') {
                table.dorisHost = ds.host + ':' + (ds.port || 9030);
                table.dorisDatabase = ds.database;
                table.dorisUsername = ds.username;
                table.dorisPassword = ds.password;
                table.dorisUrl = 'jdbc:mysql://' + ds.host + ':' + (ds.port || 9030) + '/' + ds.database;
                table.dorisDriver = 'com.mysql.cj.jdbc.Driver';
                
                if (!table.tableName) {
                    table.tableName = ds.name + '_sink';
                }
                table.dorisTable = table.tableName;
            } else if (table.connectorType === 'starrocks' || table.connectorType === 'starrocks-cdc') {
                table.starrocksHost = ds.host + ':' + (ds.port || 9030);
                table.starrocksDatabase = ds.database;
                table.starrocksUsername = ds.username;
                table.starrocksPassword = ds.password;
                table.starrocksUrl = 'jdbc:mysql://' + ds.host + ':' + (ds.port || 9030) + '/' + ds.database;
                table.starrocksDriver = 'com.mysql.cj.jdbc.Driver';
                
                if (!table.tableName) {
                    table.tableName = ds.name + '_sink';
                }
                table.starrocksTable = table.tableName;
            } else if (table.connectorType === 'oracle-cdc') {
                table.oracleHost = ds.host + ':' + ds.port;
                table.oracleServiceName = ds.database;
                table.oracleUsername = ds.username;
                table.oraclePassword = ds.password;
                
                if (!table.tableName) {
                    table.tableName = ds.name + '_source';
                }
            } else if (table.connectorType === 'sqlserver-cdc') {
                table.sqlserverHost = ds.host + ':' + ds.port;
                table.sqlserverDatabase = ds.database;
                table.sqlserverUsername = ds.username;
                table.sqlserverPassword = ds.password;
                
                if (!table.tableName) {
                    table.tableName = ds.name + '_source';
                }
            } else if (table.connectorType === 'postgres-cdc') {
                table.postgresHost = ds.host + ':' + ds.port;
                table.postgresDatabase = ds.database;
                table.postgresUsername = ds.username;
                table.postgresPassword = ds.password;
                
                if (!table.tableName) {
                    table.tableName = ds.name + '_source';
                }
            }
        }

        function updateJdbcUrl(table) {
            if (table.connectorType === 'jdbc' || table.connectorType === 'mysql-cdc') {
                const host = table.jdbcHost || 'localhost';
                const port = table.jdbcPort || '3306';
                const db = table.jdbcDatabase || '';
                table.jdbcUrl = `jdbc:mysql://${host}:${port}/${db}`;
                autoFillDriver(table);
            }
        }

        function autoFillDriver(table) {
            const driverMappings = {
                'mysql': 'com.mysql.cj.jdbc.Driver',
                'postgresql': 'org.postgresql.Driver',
                'oracle': 'oracle.jdbc.OracleDriver',
                'sqlserver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                'doris': 'com.mysql.cj.jdbc.Driver',
                'starrocks': 'com.mysql.cj.jdbc.Driver',
                'clickhouse': 'com.clickhouse.jdbc.ClickHouseDriver'
            };
            
            if (table.jdbcUrl) {
                for (const [dbType, driver] of Object.entries(driverMappings)) {
                    if (table.jdbcUrl.toLowerCase().includes(dbType)) {
                        table.jdbcDriver = driver;
                        return;
                    }
                }
            }
        }

        function autoFillDriverCommon() {
            const driverMappings = {
                'mysql': 'com.mysql.cj.jdbc.Driver',
                'postgresql': 'org.postgresql.Driver',
                'oracle': 'oracle.jdbc.OracleDriver',
                'sqlserver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                'doris': 'com.mysql.cj.jdbc.Driver',
                'starrocks': 'com.mysql.cj.jdbc.Driver',
                'clickhouse': 'com.clickhouse.jdbc.ClickHouseDriver'
            };
            
            if (sinkCommonConfig.value.jdbcUrl) {
                for (const [dbType, driver] of Object.entries(driverMappings)) {
                    if (sinkCommonConfig.value.jdbcUrl.toLowerCase().includes(dbType)) {
                        sinkCommonConfig.value.jdbcDriver = driver;
                        return;
                    }
                }
            }
        }

        function getDatasourcesForConnector(connectorType) {
            const typeMapping = {
                'jdbc': ['mysql', 'postgresql', 'oracle', 'sqlserver', 'oceanbase'],
                'jdbc-upsert': ['mysql', 'postgresql', 'oracle', 'sqlserver', 'oceanbase'],
                'postgres-jdbc': ['postgresql'],
                'oracle-jdbc': ['oracle'],
                'sqlserver-jdbc': ['sqlserver'],
                'doris': ['doris'],
                'starrocks': ['starrocks'],
                'clickhouse': ['clickhouse']
            };
            const allowedTypes = typeMapping[connectorType] || [];
            return datasources.value.filter(ds => allowedTypes.includes(ds.type));
        }

        function handleCommonConnectorChange() {
            autoFillDriverCommon();
        }

        async function applyCommonDatasource(dsId) {
            if (!dsId) return;
            
            const ds = datasources.value.find(d => d.id === dsId);
            if (!ds) return;
            
            const connectorType = sinkCommonConfig.value.connectorType;
            
            if (['jdbc', 'jdbc-upsert', 'postgres-jdbc', 'oracle-jdbc', 'sqlserver-jdbc'].includes(connectorType)) {
                sinkCommonConfig.value.jdbcUrl = 'jdbc:' + ds.type + '://' + ds.host + ':' + (ds.port || 3306) + '/' + ds.database;
                sinkCommonConfig.value.jdbcUsername = ds.username;
                sinkCommonConfig.value.jdbcPassword = ds.password;
                autoFillDriverCommon();
            } else if (connectorType === 'doris') {
                sinkCommonConfig.value.dorisUrl = 'jdbc:mysql://' + ds.host + ':' + (ds.port || 9030) + '/' + ds.database;
                sinkCommonConfig.value.dorisUsername = ds.username;
                sinkCommonConfig.value.dorisPassword = ds.password;
            } else if (connectorType === 'starrocks') {
                sinkCommonConfig.value.starrocksUrl = 'jdbc:mysql://' + ds.host + ':' + (ds.port || 9030) + '/' + ds.database;
                sinkCommonConfig.value.starrocksUsername = ds.username;
                sinkCommonConfig.value.starrocksPassword = ds.password;
            } else if (connectorType === 'clickhouse') {
                sinkCommonConfig.value.clickhouseUrl = 'jdbc:clickhouse://' + ds.host + ':' + (ds.port || 8123) + '/' + ds.database;
                sinkCommonConfig.value.clickhouseUsername = ds.username;
                sinkCommonConfig.value.clickhousePassword = ds.password;
            }
        }

        function applyCommonConfigToAllSinks() {
            const config = sinkCommonConfig.value;
            sinkTables.value.forEach(table => {
                table.connectorType = config.connectorType;
                table.datasourceId = config.datasourceId;
                
                if (['jdbc', 'jdbc-upsert', 'postgres-jdbc', 'oracle-jdbc', 'sqlserver-jdbc'].includes(config.connectorType)) {
                    table.jdbcUrl = config.jdbcUrl;
                    table.jdbcDriver = config.jdbcDriver;
                    table.jdbcUsername = config.jdbcUsername;
                    table.jdbcPassword = config.jdbcPassword;
                } else if (config.connectorType === 'doris') {
                    table.dorisUrl = config.dorisUrl;
                    table.dorisDriver = config.dorisDriver;
                    table.dorisUsername = config.dorisUsername;
                    table.dorisPassword = config.dorisPassword;
                } else if (config.connectorType === 'starrocks') {
                    table.starrocksUrl = config.starrocksUrl;
                    table.starrocksDriver = config.starrocksDriver;
                    table.starrocksUsername = config.starrocksUsername;
                    table.starrocksPassword = config.starrocksPassword;
                } else if (config.connectorType === 'clickhouse') {
                    table.clickhouseUrl = config.clickhouseUrl;
                    table.clickhouseDriver = config.clickhouseDriver;
                    table.clickhouseUsername = config.clickhouseUsername;
                    table.clickhousePassword = config.clickhousePassword;
                } else if (config.connectorType === 'kafka') {
                    table.kafkaBootstrapServers = config.kafkaBootstrapServers;
                    table.format = config.kafkaFormat;
                }
            });
            ElMessage.success('已将公共配置应用到所有汇表');
        }

        function getSourceDatasourcesForConnector(connectorType) {
            const typeMap = {
                'mysql-cdc': 'mysql',
                'postgres-cdc': 'postgresql',
                'sqlserver-cdc': 'sqlserver',
                'oracle-cdc': 'oracle',
                'tidb-cdc': 'tidb',
                'oceanbase-cdc': 'oceanbase',
                'jdbc': ['mysql', 'postgresql', 'oracle', 'sqlserver']
            };
            const targetType = typeMap[connectorType];
            if (Array.isArray(targetType)) {
                return datasources.value.filter(ds => targetType.includes(ds.type));
            }
            return datasources.value.filter(ds => ds.type === targetType);
        }

        function handleSourceCommonConnectorChange() {
            const connectorType = sourceCommonConfig.value.connectorType;
            const portMap = {
                'mysql-cdc': 3306,
                'postgres-cdc': 5432,
                'sqlserver-cdc': 1433,
                'oracle-cdc': 1521,
                'tidb-cdc': 4000,
                'oceanbase-cdc': 2881
            };
            sourceCommonConfig.value.port = portMap[connectorType] || 3306;
        }

        function updateSourceCommonHostPort() {
            const hostPort = sourceCommonConfig.value.host;
            if (hostPort && hostPort.includes(':')) {
                const parts = hostPort.split(':');
                sourceCommonConfig.value.host = parts[0];
                sourceCommonConfig.value.port = parseInt(parts[1]) || 3306;
            }
        }

        function applySourceCommonDatasource(dsId) {
            if (!dsId) return;
            
            const ds = datasources.value.find(d => d.id === dsId);
            if (!ds) return;
            
            sourceCommonConfig.value.host = ds.host + ':' + ds.port;
            sourceCommonConfig.value.database = ds.database;
            sourceCommonConfig.value.username = ds.username;
            sourceCommonConfig.value.password = ds.password;
        }

        function applyCommonConfigToAllSources() {
            const config = sourceCommonConfig.value;
            sourceTables.value.forEach(table => {
                table.connectorType = config.connectorType;
                table.datasourceId = config.datasourceId;
                table.cdcScanMode = config.scanMode;
                
                if (config.connectorType === 'mysql-cdc') {
                    table.mysqlHost = config.host;
                    table.mysqlDatabase = config.database;
                    table.mysqlUsername = config.username;
                    table.mysqlPassword = config.password;
                    table.mysqlServerTimezone = config.timezone;
                    table.cdcIncrementalSnapshot = config.incrementalSnapshot;
                } else if (config.connectorType === 'postgres-cdc') {
                    table.postgresHost = config.host;
                    table.postgresDatabase = config.database;
                    table.postgresUsername = config.username;
                    table.postgresPassword = config.password;
                    table.postgresSchema = config.postgresSchema;
                    table.postgresSlotName = config.postgresSlotName;
                } else if (config.connectorType === 'sqlserver-cdc') {
                    table.sqlserverHost = config.host;
                    table.sqlserverDatabase = config.database;
                    table.sqlserverUsername = config.username;
                    table.sqlserverPassword = config.password;
                    table.sqlserverSchema = config.sqlserverSchema;
                    table.cdcIncrementalSnapshot = config.incrementalSnapshot;
                } else if (config.connectorType === 'oracle-cdc') {
                    table.oracleHost = config.host;
                    table.oracleUsername = config.username;
                    table.oraclePassword = config.password;
                } else if (config.connectorType === 'tidb-cdc') {
                    table.tidbHost = config.host;
                    table.tidbDatabase = config.database;
                    table.tidbUsername = config.username;
                    table.tidbPassword = config.password;
                    table.tidbPdAddresses = config.tidbPdAddresses;
                } else if (config.connectorType === 'oceanbase-cdc') {
                    table.oceanbaseHost = config.host;
                    table.oceanbaseDatabase = config.database;
                    table.oceanbaseUsername = config.username;
                    table.oceanbasePassword = config.password;
                    table.oceanbaseTenant = config.oceanbaseTenant;
                    table.cdcIncrementalSnapshot = config.incrementalSnapshot;
                }
            });
            ElMessage.success('已将公共配置应用到所有源表');
        }

        async function refreshSourceTableFields(table) {
            const config = sourceCommonConfig.value;
            const connectorType = config.connectorType;
            
            let connectionInfo = {};
            let tableName = table.physicalTableName || table.tableName;
            
            if (!tableName) {
                ElMessage.warning('请先输入物理表名');
                return;
            }
            
            if (config.datasourceId) {
                try {
                    const response = await fetch('/api/datasources/' + config.datasourceId + '/tables/' + encodeURIComponent(tableName) + '/columns');
                    const data = await response.json();
                    if (data.columns && data.columns.length > 0) {
                        table.fields = data.columns.map(col => ({
                            name: col.name,
                            type: col.type || 'STRING',
                            precision: '',
                            scale: '',
                            primaryKey: col.primaryKey || false
                        }));
                        ElMessage.success('已获取表结构，共 ' + data.columns.length + ' 个字段');
                    } else {
                        ElMessage.warning('未获取到字段信息');
                    }
                } catch (e) {
                    console.error('获取表字段失败:', e);
                    ElMessage.error('获取表结构失败: ' + e.message);
                }
                return;
            }
            
            if (connectorType === 'mysql-cdc') {
                connectionInfo = {
                    host: config.host.split(':')[0],
                    port: config.host.split(':')[1] || 3306,
                    database: config.database,
                    username: config.username,
                    password: config.password,
                    type: 'mysql'
                };
            } else if (connectorType === 'postgres-cdc') {
                connectionInfo = {
                    host: config.host.split(':')[0],
                    port: config.host.split(':')[1] || 5432,
                    database: config.database,
                    username: config.username,
                    password: config.password,
                    schema: config.postgresSchema,
                    type: 'postgresql'
                };
            } else if (connectorType === 'sqlserver-cdc') {
                connectionInfo = {
                    host: config.host.split(':')[0],
                    port: config.host.split(':')[1] || 1433,
                    database: config.database,
                    username: config.username,
                    password: config.password,
                    schema: config.sqlserverSchema,
                    type: 'sqlserver'
                };
            } else if (connectorType === 'tidb-cdc') {
                connectionInfo = {
                    host: config.host.split(':')[0],
                    port: config.host.split(':')[1] || 4000,
                    database: config.database,
                    username: config.username,
                    password: config.password,
                    type: 'tidb'
                };
            } else if (connectorType === 'oceanbase-cdc') {
                connectionInfo = {
                    host: config.host.split(':')[0],
                    port: config.host.split(':')[1] || 2881,
                    database: config.database,
                    username: config.username,
                    password: config.password,
                    tenant: config.oceanbaseTenant,
                    type: 'oceanbase'
                };
            } else if (connectorType === 'oracle-cdc') {
                connectionInfo = {
                    host: config.host.split(':')[0],
                    port: config.host.split(':')[1] || 1521,
                    serviceName: config.database,
                    username: config.username,
                    password: config.password,
                    type: 'oracle'
                };
            }
            
            try {
                const response = await fetch('/api/metadata/columns?table_name=' + encodeURIComponent(tableName), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        host: connectionInfo.host,
                        port: connectionInfo.port,
                        database: connectionInfo.database,
                        username: config.username,
                        password: config.password
                    })
                });
                
                if (response.ok) {
                    const result = await response.json();
                    if (result.columns && result.columns.length > 0) {
                        table.fields = result.columns.map(f => ({
                            name: f.name,
                            type: f.type || 'STRING',
                            precision: f.precision || '',
                            scale: f.scale || '',
                            primaryKey: f.primaryKey || false
                        }));
                        ElMessage.success('已获取表结构，共 ' + result.columns.length + ' 个字段');
                    } else {
                        ElMessage.warning('未获取到字段信息');
                    }
                } else {
                    ElMessage.error('获取表结构失败');
                }
            } catch (error) {
                console.error('获取表结构失败:', error);
                ElMessage.error('获取表结构失败: ' + error.message);
            }
        }

        async function refreshSinkTableFields(table) {
            const config = sinkCommonConfig.value;
            const connectorType = config.connectorType;
            
            let connectionInfo = {};
            let tableName = table.physicalTableName || table.tableName;
            
            if (!tableName) {
                ElMessage.warning('请先输入物理表名');
                return;
            }
            
            if (config.datasourceId) {
                try {
                    const response = await fetch('/api/datasources/' + config.datasourceId + '/tables/' + encodeURIComponent(tableName) + '/columns');
                    const data = await response.json();
                    if (data.columns && data.columns.length > 0) {
                        table.fields = data.columns.map(col => ({
                            name: col.name,
                            type: col.type || 'STRING',
                            precision: '',
                            scale: '',
                            primaryKey: col.primaryKey || false
                        }));
                        ElMessage.success('已获取表结构，共 ' + data.columns.length + ' 个字段');
                    } else {
                        ElMessage.warning('未获取到字段信息');
                    }
                } catch (e) {
                    console.error('获取表字段失败:', e);
                    ElMessage.error('获取表结构失败: ' + e.message);
                }
                return;
            }
            
            if (connectorType === 'doris') {
                const dorisUrl = config.dorisUrl || '';
                const urlMatch = dorisUrl.replace('jdbc:mysql://', '').match(/^([^:]+):(\d+)\/(.+)$/);
                if (urlMatch) {
                    connectionInfo = {
                        host: urlMatch[1],
                        port: urlMatch[2],
                        database: urlMatch[3],
                        username: config.dorisUsername,
                        password: config.dorisPassword,
                        type: 'doris'
                    };
                }
            } else if (connectorType === 'starrocks') {
                const starrocksUrl = config.starrocksUrl || '';
                const urlMatch = starrocksUrl.replace('jdbc:mysql://', '').match(/^([^:]+):(\d+)\/(.+)$/);
                if (urlMatch) {
                    connectionInfo = {
                        host: urlMatch[1],
                        port: urlMatch[2],
                        database: urlMatch[3],
                        username: config.starrocksUsername,
                        password: config.starrocksPassword,
                        type: 'starrocks'
                    };
                }
            } else if (connectorType === 'clickhouse') {
                const clickhouseUrl = config.clickhouseUrl || '';
                const urlMatch = clickhouseUrl.replace('jdbc:clickhouse://', '').match(/^([^:]+):(\d+)\/(.+)$/);
                if (urlMatch) {
                    connectionInfo = {
                        host: urlMatch[1],
                        port: urlMatch[2],
                        database: urlMatch[3],
                        username: config.clickhouseUsername,
                        password: config.clickhousePassword,
                        type: 'clickhouse'
                    };
                }
            } else if (['jdbc', 'jdbc-upsert'].includes(connectorType)) {
                const jdbcUrl = config.jdbcUrl || '';
                let host = '', port = '', database = '', type = 'mysql';
                
                if (jdbcUrl.includes('mysql')) {
                    type = 'mysql';
                    const match = jdbcUrl.match(/jdbc:mysql:\/\/([^:]+):(\d+)\/(.+)/);
                    if (match) { host = match[1]; port = match[2]; database = match[3]; }
                } else if (jdbcUrl.includes('postgresql')) {
                    type = 'postgresql';
                    const match = jdbcUrl.match(/jdbc:postgresql:\/\/([^:]+):(\d+)\/(.+)/);
                    if (match) { host = match[1]; port = match[2]; database = match[3]; }
                } else if (jdbcUrl.includes('sqlserver')) {
                    type = 'sqlserver';
                    const match = jdbcUrl.match(/jdbc:sqlserver:\/\/([^:]+):(\d+);databaseName=(.+)/);
                    if (match) { host = match[1]; port = match[2]; database = match[3]; }
                } else if (jdbcUrl.includes('oracle')) {
                    type = 'oracle';
                    const match = jdbcUrl.match(/jdbc:oracle:thin:@([^:]+):(\d+):(.+)/);
                    if (match) { host = match[1]; port = match[2]; database = match[3]; }
                }
                
                connectionInfo = {
                    host, port, database,
                    username: config.jdbcUsername,
                    password: config.jdbcPassword,
                    type
                };
            }
            
            if (!connectionInfo.host) {
                ElMessage.warning('请先配置公共数据库连接信息');
                return;
            }
            
            try {
                const response = await fetch('/api/metadata/columns?table_name=' + encodeURIComponent(tableName), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        host: connectionInfo.host,
                        port: connectionInfo.port,
                        database: connectionInfo.database,
                        username: connectionInfo.username,
                        password: connectionInfo.password
                    })
                });
                
                if (response.ok) {
                    const result = await response.json();
                    if (result.columns && result.columns.length > 0) {
                        table.fields = result.columns.map(f => ({
                            name: f.name,
                            type: f.type || 'STRING',
                            precision: f.precision || '',
                            scale: f.scale || '',
                            primaryKey: f.primaryKey || false
                        }));
                        ElMessage.success('已获取表结构，共 ' + result.columns.length + ' 个字段');
                    } else {
                        ElMessage.warning('未获取到字段信息');
                    }
                } else {
                    ElMessage.error('获取表结构失败');
                }
            } catch (error) {
                console.error('获取表结构失败:', error);
                ElMessage.error('获取表结构失败: ' + error.message);
            }
        }

        function handleSinkConnectorChange(table) {
            if (!table.connectorType) return;
            autoFillDriver(table);
            
            if (sourceTables.value.length > 0) {
                const source = sourceTables.value[0];
                if (!table.tableName && source.tableName) {
                    table.tableName = source.tableName + '_sink';
                }
                if (table.fields.length === 0 && source.fields.length > 0) {
                    table.fields = source.fields.map(f => ({
                        name: f.name, type: f.type, precision: f.precision, scale: f.scale, primaryKey: f.primaryKey
                    }));
                }
                if (table.connectorType === 'jdbc' || table.connectorType === 'jdbc-upsert') {
                    if (!table.jdbcTable && source.tableName) table.jdbcTable = source.tableName;
                } else if (table.connectorType === 'starrocks') {
                    if (!table.starrocksTable && source.tableName) table.starrocksTable = source.tableName;
                } else if (table.connectorType === 'doris') {
                    if (!table.dorisTable && source.tableName) table.dorisTable = source.tableName;
                } else if (table.connectorType === 'clickhouse') {
                    if (!table.clickhouseTable && source.tableName) table.clickhouseTable = source.tableName;
                } else if (table.connectorType === 'kafka') {
                    if (!table.kafkaTopic && source.tableName) table.kafkaTopic = source.tableName;
                }
            }
        }

        function onQueryTypeChange(query) {
            if (query.queryType !== 'join') {
                query.joinTable = '';
                query.joinCondition = '';
            }
            if (query.queryType !== 'window') {
                query.windowTimeField = '';
            }
            if (query.queryType !== 'aggregate') {
                query.aggregates = [];
                query.havingClause = '';
            }
        }

        function onQuerySourceTableChange(query, tableName) {
            const fields = getFieldsByTable(tableName);
            if (fields.length > 0) {
                query.selectFields = fields.slice(0, 5).map(f => f.name);
            }
        }

        async function openSourceCommonTableSelection() {
            const config = sourceCommonConfig.value;
            if (!config.host || !config.username || !config.database) {
                ElMessage.warning('请先填写完整的数据库连接信息，或选择已保存的数据源');
                return;
            }
            
            currentTable.value = null;
            currentTableType.value = 'source';
            tableSelectionVisible.value = true;
            loadingTables.value = true;
            selectedTables.value = [];
            
            try {
                let tables = [];
                
                if (config.datasourceId) {
                    const response = await fetch('/api/datasources/' + config.datasourceId + '/tables');
                    const data = await response.json();
                    tables = data.tables || [];
                } else {
                    const hostPort = config.host || '';
                    const parts = hostPort.split(':');
                    const host = parts[0] || '';
                    const port = parseInt(parts[1]) || 3306;
                    const database = config.database || '';
                    const username = config.username || '';
                    const password = config.password || '';

                    const response = await fetch('/api/metadata/tables', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ host, port, database, username, password })
                    });
                    const data = await response.json();
                    tables = data.tables || [];
                }

                tableList.value = tables.map(t => ({ table_name: t, table_type: 'TABLE' }));
                if (tableList.value.length === 0) {
                    ElMessage.info('该数据库没有表');
                } else {
                    ElMessage.success('获取表列表成功');
                }
            } catch (error) {
                ElMessage.error('获取表列表失败: ' + error.message);
            } finally {
                loadingTables.value = false;
            }
        }

        async function openTableSelection(table, type) {
            currentTable.value = table;
            currentTableType.value = type || 'source';
            tableSelectionVisible.value = true;
            loadingTables.value = true;
            selectedTables.value = [];
            try {
                let tables = [];
                
                const config = type === 'source' ? sourceCommonConfig.value : sinkCommonConfig.value;
                const connectorType = table.connectorType || config.connectorType;
                
                if (config.datasourceId) {
                    const response = await fetch('/api/datasources/' + config.datasourceId + '/tables');
                    const data = await response.json();
                    tables = data.tables || [];
                } else {
                    let host, port, database, username, password;
                    
                    if (type === 'source') {
                        const hostPort = config.host || '';
                        const parts = hostPort.split(':');
                        host = parts[0] || '';
                        port = parseInt(parts[1]) || 3306;
                        database = config.database || '';
                        username = config.username || '';
                        password = config.password || '';
                    } else {
                        if (connectorType === 'doris') {
                            const dorisUrl = config.dorisUrl || '';
                            const match = dorisUrl.match(/jdbc:mysql:\/\/([^:]+):(\d+)\/(.+)/);
                            if (match) {
                                host = match[1];
                                port = match[2];
                                database = match[3];
                            }
                            username = config.dorisUsername;
                            password = config.dorisPassword;
                        } else if (connectorType === 'starrocks') {
                            const starrocksUrl = config.starrocksUrl || '';
                            const match = starrocksUrl.match(/jdbc:mysql:\/\/([^:]+):(\d+)\/(.+)/);
                            if (match) {
                                host = match[1];
                                port = match[2];
                                database = match[3];
                            }
                            username = config.starrocksUsername;
                            password = config.starrocksPassword;
                        } else if (connectorType === 'clickhouse') {
                            const clickhouseUrl = config.clickhouseUrl || '';
                            const match = clickhouseUrl.match(/jdbc:clickhouse:\/\/([^:]+):(\d+)\/(.+)/);
                            if (match) {
                                host = match[1];
                                port = match[2];
                                database = match[3];
                            }
                            username = config.clickhouseUsername;
                            password = config.clickhousePassword;
                        } else if (['jdbc', 'jdbc-upsert'].includes(connectorType)) {
                            const jdbcUrl = config.jdbcUrl || '';
                            if (jdbcUrl.includes('mysql')) {
                                const match = jdbcUrl.match(/jdbc:mysql:\/\/([^:]+):(\d+)\/(.+)/);
                                if (match) { host = match[1]; port = match[2]; database = match[3]; }
                            } else if (jdbcUrl.includes('postgresql')) {
                                const match = jdbcUrl.match(/jdbc:postgresql:\/\/([^:]+):(\d+)\/(.+)/);
                                if (match) { host = match[1]; port = match[2]; database = match[3]; }
                            } else if (jdbcUrl.includes('sqlserver')) {
                                const match = jdbcUrl.match(/jdbc:sqlserver:\/\/([^:]+):(\d+);databaseName=(.+)/);
                                if (match) { host = match[1]; port = match[2]; database = match[3]; }
                            } else if (jdbcUrl.includes('oracle')) {
                                const match = jdbcUrl.match(/jdbc:oracle:thin:@([^:]+):(\d+):(.+)/);
                                if (match) { host = match[1]; port = match[2]; database = match[3]; }
                            }
                            username = config.jdbcUsername;
                            password = config.jdbcPassword;
                        }
                    }

                    if (!host || !username || !database) {
                        ElMessage.warning('请先填写完整的数据库连接信息，或选择已保存的数据源');
                        loadingTables.value = false;
                        return;
                    }

                    const response = await fetch('/api/metadata/tables', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ host, port, database, username, password })
                    });
                    const data = await response.json();
                    tables = data.tables || [];
                }

                tableList.value = tables.map(t => ({ table_name: t, table_type: 'TABLE' }));
                if (tableList.value.length === 0) {
                    ElMessage.info('该数据库没有表');
                } else {
                    ElMessage.success('获取表列表成功');
                }
            } catch (error) {
                ElMessage.error('获取表列表失败: ' + error.message);
                tableList.value = [];
            } finally {
                loadingTables.value = false;
            }
        }

        function handleTableSelectionChange(selection) {
            selectedTables.value = selection;
        }

        async function confirmTableSelection() {
            if (selectedTables.value.length === 0) return;
            
            if (!currentTable.value) {
                const tableNames = selectedTables.value.map(t => t.table_name);
                await selectMultipleTablesFromCommon(tableNames);
            } else if (selectedTables.value.length === 1) {
                await selectTable(selectedTables.value[0].table_name);
            } else {
                const tableNames = selectedTables.value.map(t => t.table_name);
                await selectMultipleTables(tableNames);
            }
        }

        async function selectMultipleTablesFromCommon(tableNames) {
            const config = sourceCommonConfig.value;
            const dbName = config.database;
            
            sourceTables.value = sourceTables.value.filter(t => t.tableName && t.tableName.trim() !== '');
            
            for (let i = 0; i < tableNames.length; i++) {
                const tableName = tableNames[i];
                
                let flinkTableName = tableName;
                let physicalTableName = tableName;
                
                const hasDbPrefix = tableName.includes('.');
                const cleanTableName = hasDbPrefix ? tableName.split('.').pop() : tableName;
                flinkTableName = dbName + '_' + cleanTableName;
                
                const newTable = {
                    tableName: flinkTableName,
                    physicalTableName: cleanTableName,
                    connectorType: config.connectorType,
                    datasourceId: config.datasourceId,
                    fields: [],
                    watermarkField: '',
                    watermarkDelay: '0',
                    eventTimeField: '',
                    cdcScanMode: config.scanMode,
                    cdcIncrementalSnapshot: config.incrementalSnapshot
                };
                
                try {
                    const hostPort = config.host || '';
                    const parts = hostPort.split(':');
                    const host = parts[0] || '';
                    const port = parseInt(parts[1]) || 3306;
                    
                    const response = await fetch('/api/metadata/columns?table_name=' + encodeURIComponent(cleanTableName), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            host, port,
                            database: config.database,
                            username: config.username,
                            password: config.password
                        })
                    });
                    
                    if (response.ok) {
                        const result = await response.json();
                        if (result.columns && result.columns.length > 0) {
                            newTable.fields = result.columns.map(f => ({
                                name: f.name,
                                type: f.type || 'STRING',
                                precision: f.precision || '',
                                scale: f.scale || '',
                                primaryKey: f.primaryKey || false
                            }));
                        }
                    }
                } catch (e) {
                    console.warn('获取表字段失败:', e);
                }
                
                sourceTables.value.push(newTable);
                
                sinkTables.value = sinkTables.value.filter(t => t.tableName && t.tableName.trim() !== '');
                
                const sinkTable = createSinkTableFromSource(newTable, flinkTableName);
                sinkTables.value.push(sinkTable);
                
                queries.value = queries.value.filter(q => q.sinkTable && q.sinkTable.trim() !== '');
                
                const newQuery = {
                    sinkTable: sinkTable.tableName,
                    sourceTable: flinkTableName,
                    queryType: 'insert',
                    selectFields: newTable.fields.map(f => f.name),
                    selectedFields: newTable.fields.map(f => f.name),
                    groupByFields: [],
                    aggregates: [],
                    whereClause: '',
                    havingClause: '',
                    joinType: 'INNER',
                    joinTable: '',
                    joinCondition: '',
                    windowType: 'TUMBLE',
                    windowSize: '5 MINUTES',
                    slideSize: '1 MINUTE',
                    windowTimeField: '',
                    customSql: ''
                };
                
                queries.value.push(newQuery);
            }
            
            currentSourceTabIndex.value = String(sourceTables.value.length - 1);
            tableSelectionVisible.value = false;
            ElMessage.success('已添加 ' + tableNames.length + ' 个源表');
        }

        async function selectMultipleTables(tableNames) {
            if (!currentTable.value || tableNames.length === 0) return;
            
            const baseTable = currentTable.value;
            const dbName = getDatabaseName(baseTable);
            const isJdbc = baseTable.connectorType === 'jdbc' || baseTable.connectorType === 'jdbc-upsert';
            const isSource = currentTableType.value === 'source';
            
            for (let i = 0; i < tableNames.length; i++) {
                const tableName = tableNames[i];
                let newTable;
                
                if (i === 0) {
                    newTable = baseTable;
                } else {
                    newTable = JSON.parse(JSON.stringify(baseTable));
                    newTable.fields = [];
                }
                
                let flinkTableName = tableName;
                let connectorTableName = tableName;
                
                if (!isJdbc && dbName) {
                    const hasDbPrefix = tableName.includes('.');
                    const cleanTableName = hasDbPrefix ? tableName.split('.').pop() : tableName;
                    
                    flinkTableName = dbName + '_' + cleanTableName;
                    connectorTableName = dbName + '.' + cleanTableName;
                }
                
                newTable.tableName = flinkTableName;
                
                if (isJdbc) {
                    newTable.jdbcTable = tableName;
                } else if (newTable.connectorType === 'mysql-cdc') {
                    newTable.mysqlTable = connectorTableName;
                } else if (newTable.connectorType === 'oracle-cdc') {
                    newTable.oracleTable = connectorTableName;
                } else if (newTable.connectorType === 'sqlserver-cdc') {
                    newTable.sqlserverTable = connectorTableName;
                } else if (newTable.connectorType === 'postgres-cdc') {
                    newTable.postgresTable = connectorTableName;
                } else if (newTable.connectorType === 'tidb-cdc') {
                    newTable.tidbTable = connectorTableName;
                } else if (newTable.connectorType === 'oceanbase-cdc') {
                    newTable.oceanbaseTable = connectorTableName;
                } else if (newTable.connectorType === 'doris') {
                    newTable.dorisTable = connectorTableName;
                } else if (newTable.connectorType === 'starrocks') {
                    newTable.starrocksTable = connectorTableName;
                } else {
                    newTable.jdbcTable = tableName;
                }
                
                if (newTable.datasourceId) {
                    try {
                        const response = await fetch('/api/datasources/' + newTable.datasourceId + '/tables/' + encodeURIComponent(tableName) + '/columns');
                        const data = await response.json();
                        if (data.columns && data.columns.length > 0) {
                            newTable.fields = data.columns.map(col => ({
                                name: col.name,
                                type: col.type,
                                precision: '',
                                scale: '',
                                primaryKey: col.primaryKey || false
                            }));
                        }
                    } catch (e) {
                        console.warn('获取表字段失败:', e);
                    }
                }
                
                if (i > 0) {
                    if (isSource) {
                        sourceTables.value.push(newTable);
                    } else {
                        sinkTables.value.push(newTable);
                    }
                }
                
                if (isSource) {
                    const sinkTable = createSinkTableFromSource(newTable, flinkTableName);
                    sinkTables.value.push(sinkTable);
                    
                    const newQuery = {
                        sinkTable: sinkTable.tableName,
                        sourceTable: flinkTableName,
                        queryType: 'insert',
                        selectFields: [],
                        selectedFields: [],
                        groupByFields: [],
                        aggregates: [],
                        whereClause: '',
                        havingClause: '',
                        joinType: 'INNER',
                        joinTable: '',
                        joinCondition: '',
                        windowType: 'TUMBLE',
                        windowSize: '5 MINUTES',
                        slideSize: '1 MINUTE',
                        windowTimeField: '',
                        customSql: ''
                    };
                    
                    if (newTable.fields && newTable.fields.length > 0) {
                        const fieldsToSelect = newTable.fields.map(f => f.name);
                        newQuery.selectFields = fieldsToSelect;
                        newQuery.selectedFields = fieldsToSelect;
                    }
                    
                    queries.value.push(newQuery);
                }
            }
            
            if (isSource) {
                currentSourceTabIndex.value = String(sourceTables.value.length - 1);
                currentSinkTabIndex.value = String(sinkTables.value.length - 1);
            } else {
                currentSinkTabIndex.value = String(sinkTables.value.length - 1);
            }
            
            ElMessage.success('已添加 ' + tableNames.length + ' 个表' + (isSource ? '，并自动创建对应汇表和查询' : ''));
            tableSelectionVisible.value = false;
        }

        async function selectTable(tableName) {
            if (currentTable.value) {
                const table = currentTable.value;
                const dbName = getDatabaseName(table);
                
                let flinkTableName = tableName;
                let connectorTableName = tableName;
                
                const isJdbc = table.connectorType === 'jdbc' || table.connectorType === 'jdbc-upsert';
                if (!isJdbc && dbName) {
                    const hasDbPrefix = tableName.includes('.');
                    const cleanTableName = hasDbPrefix ? tableName.split('.').pop() : tableName;
                    
                    flinkTableName = dbName + '_' + cleanTableName;
                    connectorTableName = dbName + '.' + cleanTableName;
                }
                
                table.tableName = flinkTableName;
                
                if (isJdbc) {
                    table.jdbcTable = tableName;
                } else if (table.connectorType === 'mysql-cdc') {
                    table.mysqlTable = connectorTableName;
                } else if (table.connectorType === 'oracle-cdc') {
                    table.oracleTable = connectorTableName;
                } else if (table.connectorType === 'sqlserver-cdc') {
                    table.sqlserverTable = connectorTableName;
                } else if (table.connectorType === 'postgres-cdc') {
                    table.postgresTable = connectorTableName;
                } else if (table.connectorType === 'tidb-cdc') {
                    table.tidbTable = connectorTableName;
                } else if (table.connectorType === 'oceanbase-cdc') {
                    table.oceanbaseTable = connectorTableName;
                } else if (table.connectorType === 'doris') {
                    table.dorisTable = connectorTableName;
                } else if (table.connectorType === 'starrocks') {
                    table.starrocksTable = connectorTableName;
                } else {
                    table.jdbcTable = tableName;
                }
                
                if (table.datasourceId) {
                    try {
                        const response = await fetch('/api/datasources/' + table.datasourceId + '/tables/' + encodeURIComponent(tableName) + '/columns');
                        const data = await response.json();
                        if (data.columns && data.columns.length > 0) {
                            table.fields = data.columns.map(col => ({
                                name: col.name,
                                type: col.type,
                                precision: '',
                                scale: '',
                                primaryKey: col.primaryKey || false
                            }));
                            ElMessage.success('已加载 ' + table.fields.length + ' 个字段');
                        }
                    } catch (e) {
                        console.warn('获取表字段失败:', e);
                    }
                }
            }
            tableSelectionVisible.value = false;
        }

        function getDatabaseName(table) {
            if (table.connectorType === 'mysql-cdc') return table.mysqlDatabase;
            if (table.connectorType === 'postgres-cdc') return table.postgresDatabase;
            if (table.connectorType === 'oracle-cdc') return table.oracleServiceName;
            if (table.connectorType === 'sqlserver-cdc') return table.sqlserverDatabase;
            if (table.connectorType === 'tidb-cdc') return table.tidbDatabase;
            if (table.connectorType === 'oceanbase-cdc') return table.oceanbaseDatabase;
            return null;
        }

        async function loadFieldsFromDb(table) {
            if (!table.jdbcTable) {
                ElMessage.warning('请先选择表名');
                return;
            }
            
            let host, port, database, username, password;
            
            if (table.connectorType === 'jdbc' || table.connectorType === 'mysql-cdc') {
                host = table.jdbcHost;
                port = table.jdbcPort;
                database = table.jdbcDatabase;
                username = table.jdbcUsername;
                password = table.jdbcPassword;
            } else if (table.connectorType === 'mysql-cdc') {
                const parts = (table.mysqlHost || 'localhost:3306').split(':');
                host = parts[0];
                port = parseInt(parts[1]) || 3306;
                database = table.mysqlDatabase;
                username = table.mysqlUsername;
                password = table.mysqlPassword;
            } else if (table.connectorType === 'oracle-cdc') {
                const parts = (table.oracleHost || 'localhost:1521').split(':');
                host = parts[0];
                port = parseInt(parts[1]) || 1521;
                database = table.oracleServiceName;
                username = table.oracleUsername;
                password = table.oraclePassword;
            } else if (table.connectorType === 'sqlserver-cdc') {
                const parts = (table.sqlserverHost || 'localhost:1433').split(':');
                host = parts[0];
                port = parseInt(parts[1]) || 1433;
                database = table.sqlserverDatabase;
                username = table.sqlserverUsername;
                password = table.sqlserverPassword;
            } else if (table.connectorType === 'postgres-cdc') {
                const parts = (table.postgresHost || 'localhost:5432').split(':');
                host = parts[0];
                port = parseInt(parts[1]) || 5432;
                database = table.postgresDatabase;
                username = table.postgresUsername;
                password = table.postgresPassword;
            } else if (table.connectorType === 'doris' || table.connectorType === 'doris-cdc') {
                const parts = (table.dorisHost || 'localhost:8030').split(':');
                host = parts[0];
                port = parseInt(parts[1]) || 8030;
                database = table.dorisDatabase;
                username = table.dorisUsername;
                password = table.dorisPassword;
            } else if (table.connectorType === 'starrocks' || table.connectorType === 'starrocks-cdc') {
                const parts = (table.starrocksHost || 'localhost:9030').split(':');
                host = parts[0];
                port = parseInt(parts[1]) || 9030;
                database = table.starrocksDatabase;
                username = table.starrocksUsername;
                password = table.starrocksPassword;
            } else if (table.connectorType === 'oceanbase-cdc') {
                const parts = (table.oceanbaseHost || 'localhost:2881').split(':');
                host = parts[0];
                port = parseInt(parts[1]) || 2881;
                database = table.oceanbaseDatabase;
                username = table.oceanbaseUsername;
                password = table.oceanbasePassword;
            } else {
                ElMessage.warning('该连接器类型不支持从数据库加载字段');
                return;
            }

            try {
                const response = await fetch('/api/metadata/columns?table_name=' + encodeURIComponent(table.jdbcTable), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ host, port, database, username, password })
                });
                const data = await response.json();
                
                if (data.columns && data.columns.length > 0) {
                    const typeMapping = {
                        'varchar': 'VARCHAR',
                        'char': 'CHAR',
                        'text': 'STRING',
                        'int': 'INT',
                        'integer': 'INT',
                        'tinyint': 'TINYINT',
                        'smallint': 'SMALLINT',
                        'bigint': 'BIGINT',
                        'float': 'FLOAT',
                        'double': 'DOUBLE',
                        'decimal': 'DECIMAL',
                        'date': 'DATE',
                        'datetime': 'TIMESTAMP',
                        'timestamp': 'TIMESTAMP',
                        'time': 'TIME',
                        'boolean': 'BOOLEAN',
                        'blob': 'BINARY',
                        'binary': 'BINARY'
                    };
                    
                    table.fields = data.columns.map(col => {
                        const mysqlType = (col.type || '').toLowerCase().split('(')[0];
                        let flinkType = typeMapping[mysqlType] || 'STRING';
                        let precision = '';
                        let scale = '';
                        
                        if (col.type && col.type.includes('(')) {
                            const match = col.type.match(/\((\d+)(?:,(\d+))?\)/);
                            if (match) {
                                precision = match[1];
                                scale = match[2] || '';
                            }
                        }
                        
                        return {
                            name: col.name || col.column_name,
                            type: flinkType,
                            precision: precision,
                            scale: scale,
                            primaryKey: col.primary_key || col.key === 'PRI'
                        };
                    });
                    ElMessage.success('已加载 ' + table.fields.length + ' 个字段');
                } else {
                    ElMessage.warning('未找到字段信息');
                }
            } catch (error) {
                ElMessage.error('加载字段失败: ' + error.message);
            }
        }

        function generateFieldDef(field) {
            let def = '`' + field.name + '` ' + field.type;
            if (['VARCHAR', 'CHAR'].includes(field.type) && field.precision) {
                def += '(' + field.precision + ')';
            } else if (field.type === 'DECIMAL' && field.precision) {
                def += '(' + field.precision + (field.scale ? ',' + field.scale : '') + ')';
            } else if (['TIMESTAMP', 'TIMESTAMP_LTZ'].includes(field.type) && field.precision) {
                def += '(' + field.precision + ')';
            }
            return def;
        }

        function generateSourceTableSQL(table) {
            let sql = 'CREATE TABLE `' + table.tableName + '` (\n';
            const fieldDefs = table.fields.map(f => '    ' + generateFieldDef(f)).join(',\n');
            sql += fieldDefs;
            
            const primaryKeyFields = table.fields.filter(f => f.primaryKey).map(f => '`' + f.name + '`');
            if (primaryKeyFields.length > 0) {
                sql += ',\n    PRIMARY KEY (' + primaryKeyFields.join(', ') + ') NOT ENFORCED';
            }
            
            sql += '\n) WITH (\n';
            const withOptions = [];
            const connectorType = table.connectorType || sourceCommonConfig.value.connectorType;
            withOptions.push('    \'connector\' = \'' + connectorType + '\'');
            
            const config = sourceCommonConfig.value;
            const host = config.host.includes(':') ? config.host.split(':')[0] : config.host;
            const port = config.host.includes(':') ? config.host.split(':')[1] : config.port;
            const physicalTable = table.physicalTableName || table.tableName;
            
            if (connectorType === 'kafka') {
                withOptions.push('    \'topic\' = \'' + table.kafkaTopic + '\'');
                withOptions.push('    \'properties.bootstrap.servers\' = \'' + (table.kafkaBootstrapServers || config.kafkaBootstrapServers || 'localhost:9092') + '\'');
                withOptions.push('    \'properties.group.id\' = \'' + (table.kafkaGroup || config.kafkaGroup || 'flink-consumer') + '\'');
                withOptions.push('    \'scan.startup.mode\' = \'' + (table.kafkaStartMode || config.kafkaStartMode || 'latest') + '\'');
                withOptions.push('    \'format\' = \'' + (table.format || config.kafkaFormat || 'json') + '\'');
            } else if (connectorType === 'jdbc') {
                withOptions.push('    \'url\' = \'' + (table.jdbcUrl || '') + '\'');
                withOptions.push('    \'table-name\' = \'' + physicalTable + '\'');
                withOptions.push('    \'username\' = \'' + (table.jdbcUsername || config.username) + '\'');
                withOptions.push('    \'password\' = \'' + (table.jdbcPassword || config.password) + '\'');
                withOptions.push('    \'driver\' = \'' + (table.jdbcDriver || 'com.mysql.cj.jdbc.Driver') + '\'');
            } else if (connectorType === 'mysql-cdc') {
                withOptions.push('    \'hostname\' = \'' + host + '\'');
                withOptions.push('    \'port\' = \'' + (port || '3306') + '\'');
                withOptions.push('    \'database-name\' = \'' + (table.mysqlDatabase || config.database) + '\'');
                withOptions.push('    \'table-name\' = \'' + physicalTable + '\'');
                withOptions.push('    \'username\' = \'' + (table.mysqlUsername || config.username) + '\'');
                withOptions.push('    \'password\' = \'' + (table.mysqlPassword || config.password) + '\'');
                if (table.mysqlServerTimezone || config.timezone) withOptions.push('    \'server-time-zone\' = \'' + (table.mysqlServerTimezone || config.timezone) + '\'');
                if (table.cdcIncrementalSnapshot !== undefined ? table.cdcIncrementalSnapshot : config.incrementalSnapshot) withOptions.push('    \'scan.incremental.snapshot.enabled\' = \'true\'');
                if (table.cdcScanMode || config.scanMode) withOptions.push('    \'scan.startup.mode\' = \'' + (table.cdcScanMode || config.scanMode) + '\'');
            } else if (connectorType === 'oracle-cdc') {
                withOptions.push('    \'hostname\' = \'' + host + '\'');
                withOptions.push('    \'port\' = \'' + (port || '1521') + '\'');
                withOptions.push('    \'database-name\' = \'' + (table.oracleServiceName || config.database) + '\'');
                withOptions.push('    \'table-name\' = \'' + physicalTable + '\'');
                withOptions.push('    \'username\' = \'' + (table.oracleUsername || config.username) + '\'');
                withOptions.push('    \'password\' = \'' + (table.oraclePassword || config.password) + '\'');
            } else if (connectorType === 'sqlserver-cdc') {
                withOptions.push('    \'hostname\' = \'' + host + '\'');
                withOptions.push('    \'port\' = \'' + (port || '1433') + '\'');
                withOptions.push('    \'database-name\' = \'' + (table.sqlserverDatabase || config.database) + '\'');
                var sqlserverSchema = table.sqlserverSchema || config.sqlserverSchema || 'dbo';
                withOptions.push('    \'table-name\' = \'' + sqlserverSchema + '.' + physicalTable + '\'');
                withOptions.push('    \'username\' = \'' + (table.sqlserverUsername || config.username) + '\'');
                withOptions.push('    \'password\' = \'' + (table.sqlserverPassword || config.password) + '\'');
                if (table.cdcScanMode || config.scanMode) withOptions.push('    \'scan.startup.mode\' = \'' + (table.cdcScanMode || config.scanMode) + '\'');
            } else if (connectorType === 'postgres-cdc') {
                withOptions.push('    \'hostname\' = \'' + host + '\'');
                withOptions.push('    \'port\' = \'' + (port || '5432') + '\'');
                withOptions.push('    \'database-name\' = \'' + (table.postgresDatabase || config.database) + '\'');
                var postgresSchema = table.postgresSchema || config.postgresSchema || 'public';
                withOptions.push('    \'schema-name\' = \'' + postgresSchema + '\'');
                withOptions.push('    \'table-name\' = \'' + physicalTable + '\'');
                withOptions.push('    \'username\' = \'' + (table.postgresUsername || config.username) + '\'');
                withOptions.push('    \'password\' = \'' + (table.postgresPassword || config.password) + '\'');
                if (table.postgresSlotName || config.postgresSlotName) withOptions.push('    \'slot.name\' = \'' + (table.postgresSlotName || config.postgresSlotName) + '\'');
                if (table.cdcScanMode || config.scanMode) withOptions.push('    \'scan.startup.mode\' = \'' + (table.cdcScanMode || config.scanMode) + '\'');
            } else if (connectorType === 'oceanbase-cdc') {
                withOptions.push('    \'hostname\' = \'' + host + '\'');
                withOptions.push('    \'port\' = \'' + (port || '2881') + '\'');
                withOptions.push('    \'database-name\' = \'' + (table.oceanbaseDatabase || config.database) + '\'');
                withOptions.push('    \'table-name\' = \'' + physicalTable + '\'');
                withOptions.push('    \'username\' = \'' + (table.oceanbaseUsername || config.username) + '\'');
                withOptions.push('    \'password\' = \'' + (table.oceanbasePassword || config.password) + '\'');
                if (table.oceanbaseTenant || config.oceanbaseTenant) withOptions.push('    \'tenant-name\' = \'' + (table.oceanbaseTenant || config.oceanbaseTenant) + '\'');
                if (table.cdcScanMode || config.scanMode) withOptions.push('    \'scan.startup.mode\' = \'' + (table.cdcScanMode || config.scanMode) + '\'');
            } else if (connectorType === 'tidb-cdc') {
                withOptions.push('    \'hostname\' = \'' + host + '\'');
                withOptions.push('    \'port\' = \'' + (port || '4000') + '\'');
                withOptions.push('    \'database-name\' = \'' + (table.tidbDatabase || config.database) + '\'');
                withOptions.push('    \'table-name\' = \'' + physicalTable + '\'');
                withOptions.push('    \'username\' = \'' + (table.tidbUsername || config.username) + '\'');
                withOptions.push('    \'password\' = \'' + (table.tidbPassword || config.password) + '\'');
                if (table.tidbPdAddresses || config.tidbPdAddresses) withOptions.push('    \'pd-addresses\' = \'' + (table.tidbPdAddresses || config.tidbPdAddresses) + '\'');
                if (table.cdcScanMode || config.scanMode) withOptions.push('    \'scan.startup.mode\' = \'' + (table.cdcScanMode || config.scanMode) + '\'');
            } else if (connectorType === 'redis') {
                withOptions.push('    \'host\' = \'' + (table.redisHost ? table.redisHost.split(':')[0] : 'localhost') + '\'');
                withOptions.push('    \'port\' = \'' + (table.redisHost ? (table.redisHost.split(':')[1] || '6379') : '6379') + '\'');
                if (table.redisPassword) withOptions.push('    \'password\' = \'' + table.redisPassword + '\'');
                withOptions.push('    \'database\' = \'' + (table.redisDatabase || 0) + '\'');
            } else if (connectorType === 'filesystem') {
                withOptions.push('    \'path\' = \'' + (table.filePath || '') + '\'');
                withOptions.push('    \'format\' = \'' + (table.format || 'json') + '\'');
            } else if (connectorType === 'socket') {
                withOptions.push('    \'hostname\' = \'' + (table.socketHost || 'localhost') + '\'');
                withOptions.push('    \'port\' = \'' + (table.socketPort || 9999) + '\'');
                withOptions.push('    \'format\' = \'' + (table.format || 'json') + '\'');
            } else if (connectorType === 'datagen') {
                withOptions.push('    \'rows-per-second\' = \'' + (table.rowsPerSecond || 1) + '\'');
                if (table.numberOfRows) withOptions.push('    \'number-of-rows\' = \'' + table.numberOfRows + '\'');
            }
            
            sql += withOptions.join(',\n') + '\n);\n\n';
            return sql;
        }

        function generateSinkTableSQL(table) {
            let sql = 'CREATE TABLE `' + table.tableName + '` (\n';
            const fieldDefs = table.fields.map(f => '    ' + generateFieldDef(f)).join(',\n');
            sql += fieldDefs;
            
            const primaryKeyFields = table.fields.filter(f => f.primaryKey).map(f => '`' + f.name + '`');
            if (primaryKeyFields.length > 0) {
                sql += ',\n    PRIMARY KEY (' + primaryKeyFields.join(', ') + ') NOT ENFORCED';
            }
            
            sql += '\n) WITH (\n';
            const withOptions = [];
            const connectorType = table.connectorType || sinkCommonConfig.value.connectorType;
            withOptions.push('    \'connector\' = \'' + connectorType + '\'');
            
            if (connectorType === 'kafka') {
                withOptions.push('    \'topic\' = \'' + table.kafkaTopic + '\'');
                withOptions.push('    \'properties.bootstrap.servers\' = \'' + (table.kafkaBootstrapServers || sinkCommonConfig.value.kafkaBootstrapServers) + '\'');
                withOptions.push('    \'format\' = \'' + (table.format || sinkCommonConfig.value.kafkaFormat || 'json') + '\'');
                if (sinkCommonConfig.value.kafkaSemantic === 'exactly-once') {
                    withOptions.push('    \'semantic\' = \'exactly-once\'');
                    withOptions.push('    \'transaction.timeout.ms\' = \'900000\'');
                } else if (sinkCommonConfig.value.kafkaSemantic === 'none') {
                    withOptions.push('    \'semantic\' = \'none\'');
                }
            } else if (connectorType === 'jdbc' || connectorType === 'jdbc-upsert') {
                const jdbcUrl = table.jdbcUrl || sinkCommonConfig.value.jdbcUrl;
                const jdbcDriver = table.jdbcDriver || sinkCommonConfig.value.jdbcDriver;
                const jdbcUsername = table.jdbcUsername || sinkCommonConfig.value.jdbcUsername;
                const jdbcPassword = table.jdbcPassword || sinkCommonConfig.value.jdbcPassword;
                withOptions.push('    \'url\' = \'' + jdbcUrl + '\'');
                withOptions.push('    \'driver\' = \'' + jdbcDriver + '\'');
                withOptions.push('    \'table-name\' = \'' + (table.physicalTableName || table.jdbcTable || table.tableName) + '\'');
                withOptions.push('    \'username\' = \'' + jdbcUsername + '\'');
                withOptions.push('    \'password\' = \'' + jdbcPassword + '\'');
            } else if (connectorType === 'starrocks') {
                const starrocksUrl = table.starrocksUrl || sinkCommonConfig.value.starrocksUrl;
                const starrocksUsername = table.starrocksUsername || sinkCommonConfig.value.starrocksUsername;
                const starrocksPassword = table.starrocksPassword || sinkCommonConfig.value.starrocksPassword;
                const starrocksDb = starrocksUrl.split('/')[3] || 'default_db';
                let loadUrl = 'localhost:8030';
                if (starrocksUrl) {
                    const urlMatch = starrocksUrl.replace('jdbc:mysql://', '').match(/^([^:\/]+)/);
                    if (urlMatch) {
                        loadUrl = urlMatch[1] + ':8030';
                    }
                }
                withOptions.push('    \'jdbc-url\' = \'' + starrocksUrl + '\'');
                withOptions.push('    \'load-url\' = \'' + loadUrl + '\'');
                withOptions.push('    \'table.identifier\' = \'' + starrocksDb + '.' + (table.physicalTableName || table.starrocksTable || table.tableName) + '\'');
                withOptions.push('    \'username\' = \'' + starrocksUsername + '\'');
                withOptions.push('    \'password\' = \'' + starrocksPassword + '\'');
                withOptions.push('    \'sink.properties.format\' = \'json\'');
                if (sinkCommonConfig.value.enableBatch) {
                    withOptions.push('    \'sink.batch.size\' = \'' + (sinkCommonConfig.value.batchSize || 5000) + '\'');
                    withOptions.push('    \'sink.batch.interval\' = \'' + (sinkCommonConfig.value.batchInterval || 1000) + 'ms\'');
                }
            } else if (connectorType === 'clickhouse') {
                const clickhouseUrl = table.clickhouseUrl || sinkCommonConfig.value.clickhouseUrl;
                const clickhouseUsername = table.clickhouseUsername || sinkCommonConfig.value.clickhouseUsername;
                const clickhousePassword = table.clickhousePassword || sinkCommonConfig.value.clickhousePassword;
                withOptions.push('    \'url\' = \'' + clickhouseUrl + '\'');
                withOptions.push('    \'table-name\' = \'' + (table.physicalTableName || table.clickhouseTable || table.tableName) + '\'');
                withOptions.push('    \'username\' = \'' + (clickhouseUsername || 'default') + '\'');
                withOptions.push('    \'password\' = \'' + (clickhousePassword || '') + '\'');
            } else if (connectorType === 'doris') {
                const dorisUrl = table.dorisUrl || sinkCommonConfig.value.dorisUrl;
                const dorisUsername = table.dorisUsername || sinkCommonConfig.value.dorisUsername;
                const dorisPassword = table.dorisPassword || sinkCommonConfig.value.dorisPassword;
                const dorisDb = dorisUrl.split('/')[3] || 'default_db';
                let fenodes = 'localhost:8030';
                if (dorisUrl) {
                    const urlMatch = dorisUrl.replace('jdbc:mysql://', '').match(/^([^:\/]+)/);
                    if (urlMatch) {
                        fenodes = urlMatch[1] + ':8030';
                    }
                }
                withOptions.push('    \'fenodes\' = \'' + fenodes + '\'');
                withOptions.push('    \'table.identifier\' = \'' + dorisDb + '.' + (table.physicalTableName || table.dorisTable || table.tableName) + '\'');
                withOptions.push('    \'username\' = \'' + dorisUsername + '\'');
                withOptions.push('    \'password\' = \'' + dorisPassword + '\'');
                withOptions.push('    \'sink.properties.format\' = \'json\'');
                withOptions.push('    \'sink.properties.strip_outer_array\' = \'false\'');
                var dorisTableName = table.dorisTable || table.tableName;
                if (dorisTableName && dorisTableName.includes('.')) {
                    dorisTableName = dorisTableName.split('.').pop();
                }
                withOptions.push('    \'sink.label-prefix\' = \'label_' + dorisTableName + '\'');
                withOptions.push('    \'sink.check-interval\' = \'10000\'');
                if (sinkCommonConfig.value.enableBatch) {
                    withOptions.push('    \'sink.batch.size\' = \'' + (sinkCommonConfig.value.batchSize || 5000) + '\'');
                    withOptions.push('    \'sink.batch.interval\' = \'' + (sinkCommonConfig.value.batchInterval || 1000) + 'ms\'');
                }
            } else if (table.connectorType === 'filesystem') {
                withOptions.push('    \'path\' = \'' + table.filePath + '\'');
                withOptions.push('    \'format\' = \'' + table.format + '\'');
            }
            
            sql += withOptions.join(',\n') + '\n);\n\n';
            return sql;
        }

        function generateQuerySQL(query) {
            let sql = '';
            if (query.queryType === 'insert') {
                const fields = query.selectFields && query.selectFields.length > 0 ? query.selectFields.map(f => '`' + f + '`').join(', ') : '*';
                sql = 'INSERT INTO `' + query.sinkTable + '`\nSELECT ' + fields + ' FROM `' + query.sourceTable + '`';
                if (query.whereClause) sql += ' WHERE ' + query.whereClause;
                sql += ';\n\n';
            } else if (query.queryType === 'aggregate') {
                let selectFields = [...query.groupByFields];
                if (query.aggregates && query.aggregates.length > 0) {
                    query.aggregates.forEach(agg => {
                        let aggExpr = agg.function + '(' + (agg.field ? '`' + agg.field + '`' : '*') + ')';
                        if (agg.alias) aggExpr += ' AS `' + agg.alias + '`';
                        selectFields.push(aggExpr);
                    });
                }
                sql = 'INSERT INTO `' + query.sinkTable + '`\nSELECT ' + selectFields.map(f => '`' + f + '`').join(', ') + '\nFROM `' + query.sourceTable + '`\n';
                if (query.groupByFields && query.groupByFields.length > 0) sql += 'GROUP BY ' + query.groupByFields.map(f => '`' + f + '`').join(', ') + '\n';
                if (query.havingClause) sql += 'HAVING ' + query.havingClause + '\n';
                sql += ';\n\n';
            } else if (query.queryType === 'join') {
                sql = 'INSERT INTO `' + query.sinkTable + '`\nSELECT * FROM `' + query.sourceTable + '`\n' + query.joinType + ' JOIN `' + query.joinTable + '` ON ' + query.joinCondition + ';\n\n';
            } else if (query.queryType === 'window') {
                let windowFunc = query.windowType + '(TABLE `' + query.sourceTable + '`, DESCRIPTOR(`' + query.windowTimeField + '`), ' + query.windowSize + ')';
                sql = 'INSERT INTO `' + query.sinkTable + '`\nSELECT * FROM TABLE(' + windowFunc + ');\n\n';
            } else if (query.queryType === 'custom') {
                sql = query.customSql + ';\n\n';
            }
            return sql;
        }

        function generateFullSQL() {
            let sql = '';
            if (jobConfig.parallelism && jobConfig.parallelism > 0) {
                sql += `SET 'parallelism.default' = '${jobConfig.parallelism}';\n`;
            }
            if (jobConfig.checkpointInterval && jobConfig.checkpointInterval > 0) {
                sql += `SET 'execution.checkpointing.interval' = '${jobConfig.checkpointInterval}ms';\n`;
                sql += `SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';\n`;
            } else if (sourceTables.value.some(t => t.connectorType.endsWith('-cdc'))) {
                sql += `SET 'execution.checkpointing.interval' = '30000ms';\n`;
                sql += `SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';\n`;
            }
            sql += '\n';
            sourceTables.value.forEach(table => { sql += generateSourceTableSQL(table); });
            sinkTables.value.forEach(table => { sql += generateSinkTableSQL(table); });
            if (queries.value.length > 1) sql += 'BEGIN STATEMENT SET;\n\n';
            queries.value.forEach(query => { sql += generateQuerySQL(query); });
            if (queries.value.length > 1) sql += 'END;\n';
            return sql;
        }

        function showSqlPreviewDialog() {
            generatedSql.value = generateFullSQL();
            sqlPreviewVisible.value = true;
        }

        function copySql() {
            navigator.clipboard.writeText(generatedSql.value);
            ElMessage.success('SQL 已复制到剪贴板');
        }

        function generateCreateTableSql(table) {
            let tableName = table.jdbcTable || table.dorisTable || table.starrocksTable || table.clickhouseTable || table.physicalTableName || table.tableName;
            const connectorType = table.connectorType || sinkCommonConfig.value.connectorType;
            
            const isJdbc = ['jdbc', 'jdbc-upsert', 'postgres-jdbc', 'oracle-jdbc', 'sqlserver-jdbc'].includes(connectorType);
            const isDoris = connectorType === 'doris';
            const isStarRocks = connectorType === 'starrocks';
            const isClickHouse = connectorType === 'clickhouse';
            const tableType = sinkCommonConfig.value.tableType || 'UNIQUE';
            
            function getJdbcType(field, dbType) {
                let sqlType = (field.type || 'STRING').toString().toUpperCase();
                
                if (dbType === 'mysql') {
                    if (sqlType === 'STRING') {
                        const length = field.precision || '255';
                        sqlType = 'VARCHAR(' + length + ')';
                    } else if (sqlType === 'INT' || sqlType === 'INTEGER') {
                        sqlType = 'INT';
                    } else if (sqlType === 'BIGINT') {
                        sqlType = 'BIGINT';
                    } else if (sqlType === 'BOOLEAN') {
                        sqlType = 'TINYINT(1)';
                    } else if (sqlType === 'TIMESTAMP' || sqlType === 'TIMESTAMP_LTZ') {
                        sqlType = 'DATETIME';
                    } else if (sqlType === 'DATE') {
                        sqlType = 'DATE';
                    } else if (sqlType === 'DECIMAL') {
                        const p = field.precision || '10';
                        const s = field.scale || '0';
                        sqlType = 'DECIMAL(' + p + ', ' + s + ')';
                    } else if (sqlType === 'FLOAT') {
                        sqlType = 'FLOAT';
                    } else if (sqlType === 'DOUBLE') {
                        sqlType = 'DOUBLE';
                    } else if (sqlType === 'BYTES' || sqlType === 'BINARY') {
                        sqlType = 'BLOB';
                    } else if (sqlType === 'TEXT') {
                        sqlType = 'TEXT';
                    }
                } else if (dbType === 'postgres') {
                    if (sqlType === 'STRING') {
                        const length = field.precision || '255';
                        sqlType = length <= 255 ? 'VARCHAR(' + length + ')' : 'TEXT';
                    } else if (sqlType === 'INT' || sqlType === 'INTEGER') {
                        sqlType = 'INTEGER';
                    } else if (sqlType === 'BIGINT') {
                        sqlType = 'BIGINT';
                    } else if (sqlType === 'BOOLEAN') {
                        sqlType = 'BOOLEAN';
                    } else if (sqlType === 'TIMESTAMP' || sqlType === 'TIMESTAMP_LTZ') {
                        sqlType = 'TIMESTAMP';
                    } else if (sqlType === 'DATE') {
                        sqlType = 'DATE';
                    } else if (sqlType === 'DECIMAL') {
                        const p = field.precision || '10';
                        const s = field.scale || '0';
                        sqlType = 'DECIMAL(' + p + ', ' + s + ')';
                    } else if (sqlType === 'FLOAT') {
                        sqlType = 'REAL';
                    } else if (sqlType === 'DOUBLE') {
                        sqlType = 'DOUBLE PRECISION';
                    } else if (sqlType === 'BYTES' || sqlType === 'BINARY') {
                        sqlType = 'BYTEA';
                    } else if (sqlType === 'TEXT') {
                        sqlType = 'TEXT';
                    }
                } else if (dbType === 'oracle') {
                    if (sqlType === 'STRING') {
                        const length = field.precision || '255';
                        sqlType = 'VARCHAR2(' + length + ')';
                    } else if (sqlType === 'INT' || sqlType === 'INTEGER') {
                        sqlType = 'NUMBER(10)';
                    } else if (sqlType === 'BIGINT') {
                        sqlType = 'NUMBER(19)';
                    } else if (sqlType === 'BOOLEAN') {
                        sqlType = 'NUMBER(1)';
                    } else if (sqlType === 'TIMESTAMP' || sqlType === 'TIMESTAMP_LTZ') {
                        sqlType = 'TIMESTAMP';
                    } else if (sqlType === 'DATE') {
                        sqlType = 'DATE';
                    } else if (sqlType === 'DECIMAL') {
                        const p = field.precision || '10';
                        const s = field.scale || '0';
                        sqlType = 'NUMBER(' + p + ', ' + s + ')';
                    } else if (sqlType === 'FLOAT') {
                        sqlType = 'BINARY_FLOAT';
                    } else if (sqlType === 'DOUBLE') {
                        sqlType = 'BINARY_DOUBLE';
                    } else if (sqlType === 'BYTES' || sqlType === 'BINARY') {
                        sqlType = 'BLOB';
                    } else if (sqlType === 'TEXT') {
                        sqlType = 'CLOB';
                    }
                } else if (dbType === 'sqlserver') {
                    if (sqlType === 'STRING') {
                        const length = field.precision || '255';
                        sqlType = 'NVARCHAR(' + length + ')';
                    } else if (sqlType === 'INT' || sqlType === 'INTEGER') {
                        sqlType = 'INT';
                    } else if (sqlType === 'BIGINT') {
                        sqlType = 'BIGINT';
                    } else if (sqlType === 'BOOLEAN') {
                        sqlType = 'BIT';
                    } else if (sqlType === 'TIMESTAMP' || sqlType === 'TIMESTAMP_LTZ') {
                        sqlType = 'DATETIME2';
                    } else if (sqlType === 'DATE') {
                        sqlType = 'DATE';
                    } else if (sqlType === 'DECIMAL') {
                        const p = field.precision || '10';
                        const s = field.scale || '0';
                        sqlType = 'DECIMAL(' + p + ', ' + s + ')';
                    } else if (sqlType === 'FLOAT') {
                        sqlType = 'FLOAT';
                    } else if (sqlType === 'DOUBLE') {
                        sqlType = 'FLOAT';
                    } else if (sqlType === 'BYTES' || sqlType === 'BINARY') {
                        sqlType = 'VARBINARY(MAX)';
                    } else if (sqlType === 'TEXT') {
                        sqlType = 'NVARCHAR(MAX)';
                    }
                } else if (dbType === 'clickhouse') {
                    if (sqlType === 'STRING') {
                        sqlType = 'String';
                    } else if (sqlType === 'INT' || sqlType === 'INTEGER') {
                        sqlType = 'Int32';
                    } else if (sqlType === 'BIGINT') {
                        sqlType = 'Int64';
                    } else if (sqlType === 'BOOLEAN') {
                        sqlType = 'UInt8';
                    } else if (sqlType === 'TIMESTAMP' || sqlType === 'TIMESTAMP_LTZ') {
                        sqlType = 'DateTime';
                    } else if (sqlType === 'DATE') {
                        sqlType = 'Date';
                    } else if (sqlType === 'DECIMAL') {
                        const p = field.precision || '10';
                        const s = field.scale || '0';
                        sqlType = 'Decimal(' + p + ', ' + s + ')';
                    } else if (sqlType === 'FLOAT') {
                        sqlType = 'Float32';
                    } else if (sqlType === 'DOUBLE') {
                        sqlType = 'Float64';
                    } else if (sqlType === 'BYTES' || sqlType === 'BINARY') {
                        sqlType = 'String';
                    } else if (sqlType === 'TEXT') {
                        sqlType = 'String';
                    }
                } else if (dbType === 'doris' || dbType === 'starrocks') {
                    if (sqlType === 'STRING') {
                        const length = field.precision || '255';
                        sqlType = 'VARCHAR(' + length + ')';
                    } else if (sqlType === 'INT' || sqlType === 'INTEGER') {
                        sqlType = 'INT';
                    } else if (sqlType === 'BIGINT') {
                        sqlType = 'BIGINT';
                    } else if (sqlType === 'BOOLEAN') {
                        sqlType = 'BOOLEAN';
                    } else if (sqlType === 'TIMESTAMP' || sqlType === 'TIMESTAMP_LTZ') {
                        sqlType = 'DATETIME';
                    } else if (sqlType === 'DATE') {
                        sqlType = 'DATE';
                    } else if (sqlType === 'DECIMAL') {
                        const p = field.precision || '10';
                        const s = field.scale || '0';
                        sqlType = 'DECIMAL(' + p + ', ' + s + ')';
                    } else if (sqlType === 'FLOAT') {
                        sqlType = 'FLOAT';
                    } else if (sqlType === 'DOUBLE') {
                        sqlType = 'DOUBLE';
                    } else if (sqlType === 'BYTES' || sqlType === 'BINARY') {
                        sqlType = 'STRING';
                    } else if (sqlType === 'TEXT') {
                        sqlType = 'STRING';
                    }
                }
                return sqlType;
            }
            
            let columnsSql = [];
            let primaryKeys = [];
            
            let dbType = 'mysql';
            if (connectorType === 'postgres-jdbc') dbType = 'postgres';
            else if (connectorType === 'oracle-jdbc') dbType = 'oracle';
            else if (connectorType === 'sqlserver-jdbc') dbType = 'sqlserver';
            else if (connectorType === 'clickhouse') dbType = 'clickhouse';
            else if (connectorType === 'doris') dbType = 'doris';
            else if (connectorType === 'starrocks') dbType = 'starrocks';
            
            for (const field of table.fields) {
                const sqlType = getJdbcType(field, dbType);
                
                if (dbType === 'clickhouse') {
                    columnsSql.push('    `' + field.name + '` ' + sqlType);
                } else if (dbType === 'oracle' || dbType === 'sqlserver') {
                    columnsSql.push('    "' + field.name + '" ' + sqlType);
                } else {
                    columnsSql.push('    `' + field.name + '` ' + sqlType);
                }
                
                if (field.primaryKey) {
                    if (dbType === 'oracle' || dbType === 'sqlserver') {
                        primaryKeys.push('"' + field.name + '"');
                    } else {
                        primaryKeys.push('`' + field.name + '`');
                    }
                }
            }
            
            let createSql = '';
            
            if (isDoris) {
                createSql = 'CREATE TABLE `' + tableName + '` (\n';
                createSql += columnsSql.join(',\n');
                createSql += '\n) ENGINE=OLAP';
                
                if (tableType === 'UNIQUE' && primaryKeys.length > 0) {
                    createSql += '\nUNIQUE KEY (' + primaryKeys.join(', ') + ')';
                } else if (tableType === 'AGGREGATE' && primaryKeys.length > 0) {
                    createSql += '\nAGGREGATE KEY (' + primaryKeys.join(', ') + ')';
                } else if (tableType === 'DUPLICATE' && primaryKeys.length > 0) {
                    createSql += '\nDUPLICATE KEY (' + primaryKeys.join(', ') + ')';
                }
                
                if (primaryKeys.length > 0) {
                    createSql += '\nDISTRIBUTED BY HASH(' + primaryKeys.join(', ') + ') BUCKETS ' + (sinkCommonConfig.value.dorisBuckets || 10);
                } else if (table.fields.length > 0) {
                    createSql += '\nDISTRIBUTED BY HASH(`' + table.fields[0].name + '`) BUCKETS ' + (sinkCommonConfig.value.dorisBuckets || 10);
                }
                createSql += '\nPROPERTIES("replication_num" = "' + (sinkCommonConfig.value.replicationNum || 1) + '")';
            } else if (isStarRocks) {
                createSql = 'CREATE TABLE `' + tableName + '` (\n';
                createSql += columnsSql.join(',\n');
                createSql += '\n) ENGINE=OLAP';
                
                if (tableType === 'UNIQUE' && primaryKeys.length > 0) {
                    createSql += '\nPRIMARY KEY (' + primaryKeys.join(', ') + ')';
                } else if (tableType === 'AGGREGATE' && primaryKeys.length > 0) {
                    createSql += '\nAGGREGATE KEY (' + primaryKeys.join(', ') + ')';
                } else if (tableType === 'DUPLICATE' && primaryKeys.length > 0) {
                    createSql += '\nDUPLICATE KEY (' + primaryKeys.join(', ') + ')';
                }
                
                if (primaryKeys.length > 0) {
                    createSql += '\nDISTRIBUTED BY HASH(' + primaryKeys.join(', ') + ') BUCKETS ' + (sinkCommonConfig.value.dorisBuckets || 10);
                } else if (table.fields.length > 0) {
                    createSql += '\nDISTRIBUTED BY HASH(`' + table.fields[0].name + '`) BUCKETS ' + (sinkCommonConfig.value.dorisBuckets || 10);
                }
                createSql += '\nPROPERTIES("replication_num" = "' + (sinkCommonConfig.value.replicationNum || 1) + '")';
            } else if (isClickHouse) {
                createSql = 'CREATE TABLE `' + tableName + '` (\n';
                createSql += columnsSql.join(',\n');
                if (primaryKeys.length > 0) {
                    createSql += ',\n    PRIMARY KEY (' + primaryKeys.join(', ') + ')';
                }
                createSql += '\n) ENGINE = MergeTree()';
                if (primaryKeys.length > 0) {
                    createSql += '\nORDER BY (' + primaryKeys.join(', ') + ')';
                }
                createSql += '\nSETTINGS index_granularity = 8192';
            } else if (dbType === 'mysql') {
                createSql = 'CREATE TABLE `' + tableName + '` (\n';
                createSql += columnsSql.join(',\n');
                if (primaryKeys.length > 0) {
                    createSql += ',\n    PRIMARY KEY (' + primaryKeys.join(', ') + ')';
                }
                createSql += '\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4';
            } else if (dbType === 'postgres') {
                createSql = 'CREATE TABLE "' + tableName + '" (\n';
                createSql += columnsSql.join(',\n');
                if (primaryKeys.length > 0) {
                    createSql += ',\n    PRIMARY KEY (' + primaryKeys.join(', ') + ')';
                }
                createSql += '\n)';
            } else if (dbType === 'oracle') {
                createSql = 'CREATE TABLE "' + tableName + '" (\n';
                createSql += columnsSql.join(',\n');
                if (primaryKeys.length > 0) {
                    createSql += ',\n    PRIMARY KEY (' + primaryKeys.join(', ') + ')';
                }
                createSql += '\n)';
            } else if (dbType === 'sqlserver') {
                createSql = 'CREATE TABLE [' + tableName + '] (\n';
                createSql += columnsSql.join(',\n');
                if (primaryKeys.length > 0) {
                    createSql += ',\n    PRIMARY KEY (' + primaryKeys.join(', ') + ')';
                }
                createSql += '\n)';
            } else {
                createSql = 'CREATE TABLE `' + tableName + '` (\n';
                createSql += columnsSql.join(',\n');
                if (primaryKeys.length > 0) {
                    createSql += ',\n    PRIMARY KEY (' + primaryKeys.join(', ') + ')';
                }
                createSql += '\n)';
            }
            
            createSql += ';';
            return createSql;
        }

        function previewCreateTableSql(table) {
            if (!table.fields || table.fields.length === 0) {
                ElMessage.warning('请先配置字段');
                return;
            }
            currentEditingTable.value = table;
            currentCreateTableSql.value = generateCreateTableSql(table);
            createTableSqlVisible.value = true;
        }

        async function executeCreateTable() {
            if (!currentEditingTable.value || !currentCreateTableSql.value) return;
            
            const table = currentEditingTable.value;
            try {
                let createPayload = {
                    connector_type: table.connectorType,
                    table_name: table.jdbcTable || table.dorisTable || table.starrocksTable || table.tableName,
                    fields: table.fields.map(f => ({
                        name: f.name,
                        type: f.type,
                        precision: f.precision ? String(f.precision) : null,
                        scale: f.scale ? String(f.scale) : null,
                        primaryKey: f.primaryKey || false
                    })),
                    custom_sql: currentCreateTableSql.value
                };

                if (table.connectorType === 'jdbc' || table.connectorType === 'jdbc-upsert') {
                    createPayload.url = table.jdbcUrl;
                    createPayload.username = table.jdbcUsername;
                    createPayload.password = table.jdbcPassword;
                } else if (table.connectorType === 'starrocks') {
                    createPayload.url = table.starrocksUrl;
                    createPayload.username = table.starrocksUsername;
                    createPayload.password = table.starrocksPassword;
                } else if (table.connectorType === 'doris') {
                    createPayload.url = table.dorisUrl;
                    createPayload.username = table.dorisUsername;
                    createPayload.password = table.dorisPassword;
                }

                const response = await fetch('/api/metadata/tables/create-custom', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(createPayload)
                });
                const result = await response.json();
                
                if (response.ok) {
                    ElMessage.success(result.message || '建表成功');
                    createTableSqlVisible.value = false;
                } else {
                    ElMessage.error(result.detail || '建表失败');
                }
            } catch (e) {
                ElMessage.error('建表失败: ' + e.message);
            }
        }

        async function submitSql() {
            if (!jobConfig.jobName) { ElMessage.warning('请填写作业名称'); return; }
            if (sourceTables.value.length === 0) { ElMessage.warning('请至少添加一个源表'); return; }
            if (sinkTables.value.length === 0) { ElMessage.warning('请至少添加一个汇表'); return; }
            if (queries.value.length === 0) { ElMessage.warning('请至少添加一个查询'); return; }

            for (const table of sinkTables.value) {
                if (sinkCommonConfig.value.autoCreateTable) {
                    try {
                        ElMessage.info('正在尝试创建表: ' + table.tableName + ' ...');
                        
                        let createPayload = {
                            connector_type: table.connectorType || sinkCommonConfig.value.connectorType,
                            fields: table.fields.map(f => ({
                                name: f.name,
                                type: f.type,
                                precision: f.precision ? String(f.precision) : null,
                                scale: f.scale ? String(f.scale) : null,
                                primaryKey: f.primaryKey || false
                            }))
                        };

                        const connectorType = table.connectorType || sinkCommonConfig.value.connectorType;
                        if (connectorType === 'jdbc' || connectorType === 'jdbc-upsert') {
                            createPayload.table_name = table.jdbcTable || table.physicalTableName || table.tableName;
                            createPayload.url = table.jdbcUrl || sinkCommonConfig.value.jdbcUrl;
                            createPayload.username = table.jdbcUsername || sinkCommonConfig.value.jdbcUsername;
                            createPayload.password = table.jdbcPassword || sinkCommonConfig.value.jdbcPassword;
                        } else if (connectorType === 'starrocks') {
                            createPayload.table_name = table.starrocksTable || table.physicalTableName || table.tableName;
                            createPayload.url = table.starrocksUrl || sinkCommonConfig.value.starrocksUrl;
                            createPayload.username = table.starrocksUsername || sinkCommonConfig.value.starrocksUsername;
                            createPayload.password = table.starrocksPassword || sinkCommonConfig.value.starrocksPassword;
                            createPayload.replication_num = sinkCommonConfig.value.replicationNum || 1;
                            createPayload.buckets = sinkCommonConfig.value.dorisBuckets || 10;
                            createPayload.table_type = sinkCommonConfig.value.tableType || 'UNIQUE';
                        } else if (connectorType === 'doris') {
                            createPayload.table_name = table.dorisTable || table.physicalTableName || table.tableName;
                            createPayload.url = table.dorisUrl || sinkCommonConfig.value.dorisUrl;
                            createPayload.username = table.dorisUsername || sinkCommonConfig.value.dorisUsername;
                            createPayload.password = table.dorisPassword || sinkCommonConfig.value.dorisPassword;
                            createPayload.replication_num = sinkCommonConfig.value.replicationNum || 1;
                            createPayload.buckets = sinkCommonConfig.value.dorisBuckets || 10;
                            createPayload.table_type = sinkCommonConfig.value.tableType || 'UNIQUE';
                        } else {
                            continue;
                        }

                        const createResponse = await fetch('/api/metadata/tables/create', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(createPayload)
                        });
                        const createResult = await createResponse.json();
                        
                        if (createResult.status === 'skipped') {
                            ElMessage.info(createResult.message);
                        } else {
                            ElMessage.success(createResult.message);
                        }
                        
                    } catch (e) {
                        console.error('自动创建表失败:', e);
                        if (!confirm('表 ' + table.tableName + ' 自动创建失败: ' + e.message + '\n是否继续提交作业？')) {
                            return;
                        }
                    }
                }
            }

            submitting.value = true;
            try {
                const sql = generateFullSQL();
                const response = await fetch('/api/sql/submit', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ job_name: jobConfig.jobName, sql_text: sql })
                });
                const data = await response.json();
                if (response.ok) {
                    ElMessage.success('作业提交成功，Job ID: ' + (data.job_id || data.jobid || ''));
                    sqlPreviewVisible.value = false;
                } else {
                    ElMessage.error(data.detail || data.message || '提交失败');
                }
            } catch (error) {
                ElMessage.error('提交失败: ' + error.message);
            } finally {
                submitting.value = false;
            }
        }

        onMounted(async () => {
            try {
                const res = await fetch('/api/datasources');
                const data = await res.json();
                datasources.value = Array.isArray(data) ? data : [];
            } catch (e) {
                console.error('加载数据源失败', e);
            }
        });

        return {
            jobConfig, sourceTables, sinkTables, queries, datasources, submitting,
            sqlPreviewVisible, generatedSql, tableSelectionVisible, tableList,
            loadingTables, selectedTables, currentTableType, currentSourceTabIndex, currentSinkTabIndex, batchSyncEnabled,
            createTableSqlVisible, currentCreateTableSql,
            sourceConnectorGroups, sinkConnectorGroups, queryTypes, formatTypes, fieldTypes,
            sinkCommonConfig, sourceCommonConfig,
            sourceTableOptions: computed(() => sourceTables.value.map(t => ({ label: t.tableName, value: t.tableName }))),
            sinkTableOptions: computed(() => sinkTables.value.map(t => ({ label: t.tableName, value: t.tableName }))),
            addSourceTable, removeSourceTable, addSinkTable, removeSinkTable,
            addField, removeField, handlePrimaryKey, addQuery, removeQuery, clearAllQueries,
            addAggregate, removeAggregate, getFieldsByTable, getTimeFields, getOtherSourceTables,
            getConnectorLabel, syncTableName, applyDatasource, updateJdbcUrl, autoFillDriver,
            autoFillDriverCommon, getDatasourcesForConnector, handleCommonConnectorChange, applyCommonDatasource,
            applyCommonConfigToAllSinks,
            getSourceDatasourcesForConnector, handleSourceCommonConnectorChange, updateSourceCommonHostPort,
            applySourceCommonDatasource, applyCommonConfigToAllSources, refreshSourceTableFields,
            refreshSinkTableFields,
            openSourceCommonTableSelection,
            handleSinkConnectorChange, openTableSelection, selectTable, loadFieldsFromDb,
            handleTableSelectionChange, confirmTableSelection,
            onQueryTypeChange, onQuerySourceTableChange,
            previewCreateTableSql, executeCreateTable,
            showSqlPreviewDialog, copySql, submitSql
        };
    },
    template: `
<div>
    <el-card style="margin-bottom: 20px;">
        <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span>Flink 作业配置</span>
                <div>
                    <el-button @click="showSqlPreviewDialog"><el-icon><Document /></el-icon> SQL 预览</el-button>
                    <el-button type="primary" @click="submitSql" :loading="submitting"><el-icon><VideoPlay /></el-icon> 提交运行</el-button>
                </div>
            </div>
        </template>
        <el-form :model="jobConfig" label-width="120px">
            <el-row :gutter="20">
                <el-col :span="8">
                    <el-form-item label="作业名称">
                        <el-input v-model="jobConfig.jobName" placeholder="请输入作业名称" />
                    </el-form-item>
                </el-col>
                <el-col :span="8">
                    <el-form-item label="并行度">
                        <el-input-number v-model="jobConfig.parallelism" :min="1" :max="100" style="width: 100%" />
                    </el-form-item>
                </el-col>
                <el-col :span="8">
                    <el-form-item label="Checkpoint间隔(ms)">
                        <el-input-number v-model="jobConfig.checkpointInterval" :min="0" :step="1000" style="width: 100%" />
                    </el-form-item>
                </el-col>
            </el-row>
        </el-form>
    </el-card>

    <el-row :gutter="20">
        <el-col :span="24">
            <el-card>
                <template #header>
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span>源表配置</span>
                        <el-button type="primary" size="small" @click="addSourceTable"><el-icon><Plus /></el-icon> 添加源表</el-button>
                    </div>
                </template>
                <div v-if="sourceTables.length === 0" style="text-align: center; padding: 40px; color: #999;">暂无源表，点击右上角添加</div>
                <div v-if="sourceTables.length > 0">
                    <el-card shadow="never" style="margin-bottom: 15px; background: #f8f9fa;">
                        <template #header>
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="font-weight: 500;"><el-icon><Setting /></el-icon> 公共数据库连接配置（所有源表共享）</span>
                                <el-button size="small" text type="primary" @click="applyCommonConfigToAllSources">应用到所有表</el-button>
                            </div>
                        </template>
                        <el-form :model="sourceCommonConfig" label-width="100px" size="small">
                            <el-row :gutter="20">
                                <el-col :span="6">
                                    <el-form-item label="连接器类型">
                                        <el-select v-model="sourceCommonConfig.connectorType" placeholder="选择连接器" style="width: 100%;" @change="handleSourceCommonConnectorChange">
                                            <el-option-group v-for="group in sourceConnectorGroups" :key="group.label" :label="group.label">
                                                <el-option v-for="option in group.options" :key="option.value" :label="option.label" :value="option.value" />
                                            </el-option-group>
                                        </el-select>
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="选择数据源">
                                        <el-select v-model="sourceCommonConfig.datasourceId" placeholder="选择数据源" clearable @change="applySourceCommonDatasource" style="width: 100%">
                                            <el-option v-for="ds in getSourceDatasourcesForConnector(sourceCommonConfig.connectorType)" :key="ds.id" :label="ds.name" :value="ds.id">
                                                <span>{{ ds.name }}</span>
                                                <span style="float: right; color: #8492a6; font-size: 13px">{{ ds.type }}</span>
                                            </el-option>
                                        </el-select>
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="主机:端口">
                                        <el-input v-model="sourceCommonConfig.host" placeholder="localhost:3306" @input="updateSourceCommonHostPort" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="数据库">
                                        <el-input v-model="sourceCommonConfig.database" placeholder="数据库名" />
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20">
                                <el-col :span="6">
                                    <el-form-item label="时区">
                                        <el-input v-model="sourceCommonConfig.timezone" placeholder="Asia/Shanghai" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="用户名">
                                        <el-input v-model="sourceCommonConfig.username" placeholder="用户名" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="密码">
                                        <el-input v-model="sourceCommonConfig.password" type="password" placeholder="密码" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="启动模式">
                                        <el-select v-model="sourceCommonConfig.scanMode" style="width: 100%;">
                                            <el-option label="全量+增量" value="initial" />
                                            <el-option label="仅增量" value="latest-offset" />
                                        </el-select>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="['mysql-cdc', 'sqlserver-cdc', 'oceanbase-cdc'].includes(sourceCommonConfig.connectorType)">
                                <el-col :span="6">
                                    <el-form-item label="增量快照">
                                        <el-switch v-model="sourceCommonConfig.incrementalSnapshot" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label=" ">
                                        <el-button size="small" @click="openSourceCommonTableSelection"><el-icon><Search /></el-icon> 选表</el-button>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="sourceCommonConfig.connectorType === 'postgres-cdc'">
                                <el-col :span="6">
                                    <el-form-item label="Schema">
                                        <el-input v-model="sourceCommonConfig.postgresSchema" placeholder="public" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="Slot名称">
                                        <el-input v-model="sourceCommonConfig.postgresSlotName" placeholder="flink_slot" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label=" ">
                                        <el-button size="small" @click="openSourceCommonTableSelection"><el-icon><Search /></el-icon> 选表</el-button>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="sourceCommonConfig.connectorType === 'sqlserver-cdc'">
                                <el-col :span="6">
                                    <el-form-item label="Schema">
                                        <el-input v-model="sourceCommonConfig.sqlserverSchema" placeholder="dbo" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label=" ">
                                        <el-button size="small" @click="openSourceCommonTableSelection"><el-icon><Search /></el-icon> 选表</el-button>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="sourceCommonConfig.connectorType === 'tidb-cdc'">
                                <el-col :span="6">
                                    <el-form-item label="PD地址">
                                        <el-input v-model="sourceCommonConfig.tidbPdAddresses" placeholder="localhost:2379" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label=" ">
                                        <el-button size="small" @click="openSourceCommonTableSelection"><el-icon><Search /></el-icon> 选表</el-button>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="sourceCommonConfig.connectorType === 'oceanbase-cdc'">
                                <el-col :span="6">
                                    <el-form-item label="租户">
                                        <el-input v-model="sourceCommonConfig.oceanbaseTenant" placeholder="sys" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label=" ">
                                        <el-button size="small" @click="openSourceCommonTableSelection"><el-icon><Search /></el-icon> 选表</el-button>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="sourceCommonConfig.connectorType === 'oracle-cdc'">
                                <el-col :span="6">
                                    <el-form-item label=" ">
                                        <el-button size="small" @click="openSourceCommonTableSelection"><el-icon><Search /></el-icon> 选表</el-button>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="sourceCommonConfig.connectorType === 'kafka'">
                                <el-col :span="12">
                                    <el-form-item label="Bootstrap Servers">
                                        <el-input v-model="sourceCommonConfig.kafkaBootstrapServers" placeholder="localhost:9092" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="消费组ID">
                                        <el-input v-model="sourceCommonConfig.kafkaGroup" placeholder="flink-consumer" />
                                    </el-form-item>
                                </el-col>
                                <el-col :span="6">
                                    <el-form-item label="启动模式">
                                        <el-select v-model="sourceCommonConfig.kafkaStartMode" style="width: 100%;">
                                            <el-option label="最新" value="latest" />
                                            <el-option label="最早" value="earliest" />
                                            <el-option label="指定时间戳" value="timestamp" />
                                        </el-select>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                            <el-row :gutter="20" v-if="sourceCommonConfig.connectorType === 'kafka'">
                                <el-col :span="6">
                                    <el-form-item label="消息格式">
                                        <el-select v-model="sourceCommonConfig.kafkaFormat" style="width: 100%;">
                                            <el-option label="JSON" value="json" />
                                            <el-option label="Avro" value="avro" />
                                            <el-option label="CSV" value="csv" />
                                            <el-option label="Canal JSON" value="canal-json" />
                                            <el-option label="Debezium JSON" value="debezium-json" />
                                        </el-select>
                                    </el-form-item>
                                </el-col>
                            </el-row>
                        </el-form>
                    </el-card>
                    <el-tabs v-model="currentSourceTabIndex" type="card" closable @tab-remove="removeSourceTable">
                        <el-tab-pane v-for="(table, index) in sourceTables" :key="index" :name="String(index)">
                            <template #label>
                                <span :title="table.tableName || '未命名源表'">{{ table.tableName || '未命名源表' }}</span>
                            </template>
                            <el-form :model="table" label-width="120px" size="small">
                                <el-row :gutter="20">
                                    <el-col :span="8">
                                        <el-form-item label="Flink表名">
                                            <el-input v-model="table.tableName" placeholder="表名" @input="syncTableName(table, 'main')" />
                                        </el-form-item>
                                    </el-col>
                                    <el-col :span="8">
                                        <el-form-item label="物理表名">
                                            <div style="display: flex; gap: 8px;">
                                                <el-input v-model="table.physicalTableName" placeholder="数据库表名" style="flex: 1;" />
                                                <el-button size="small" @click="openTableSelection(table, 'source')"><el-icon><Search /></el-icon> 选表</el-button>
                                            </div>
                                        </el-form-item>
                                    </el-col>
                                    <el-col :span="8">
                                        <el-form-item label=" ">
                                            <el-button size="small" @click="refreshSourceTableFields(table)"><el-icon><Refresh /></el-icon> 刷新表结构</el-button>
                                        </el-form-item>
                                    </el-col>
                                </el-row>
                                <el-divider content-position="left">字段配置</el-divider>
                                <div style="margin-bottom: 10px;">
                                    <el-button size="small" @click="addField(table)"><el-icon><Plus /></el-icon> 添加字段</el-button>
                                </div>
                                <div v-for="(field, fIndex) in table.fields" :key="fIndex" style="margin-bottom: 10px;">
                                    <el-row :gutter="10">
                                        <el-col :span="6"><el-input v-model="field.name" placeholder="字段名" size="small" /></el-col>
                                        <el-col :span="5">
                                            <select v-model="field.type" class="native-select">
                                                <option v-for="t in fieldTypes" :key="t" :value="t">{{ t }}</option>
                                            </select>
                                        </el-col>
                                        <el-col :span="4"><el-input v-if="['VARCHAR','CHAR','DECIMAL','TIMESTAMP','TIMESTAMP_LTZ'].includes(field.type)" v-model="field.precision" placeholder="精度" size="small" /><span v-else style="color: #ccc;">-</span></el-col>
                                        <el-col :span="4"><el-input v-if="field.type === 'DECIMAL'" v-model="field.scale" placeholder="标度" size="small" /><span v-else style="color: #ccc;">-</span></el-col>
                                        <el-col :span="3"><el-checkbox v-model="field.primaryKey" @change="handlePrimaryKey(table, fIndex)">主键</el-checkbox></el-col>
                                        <el-col :span="2"><el-button type="danger" link @click="removeField(table, fIndex)" size="small"><el-icon><Delete /></el-icon></el-button></el-col>
                                    </el-row>
                                </div>
                                <div v-if="table.fields.length === 0" style="text-align: center; color: #999; padding: 20px; font-size: 13px;">暂无字段，请点击上方"添加字段"按钮或"刷新表结构"按钮</div>
                            </el-form>
                        </el-tab-pane>
                    </el-tabs>
                </div>
            </el-card>
        </el-col>
    </el-row>

    <el-card style="margin-top: 20px;">
        <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span>汇表配置</span>
                <el-button type="primary" size="small" @click="addSinkTable"><el-icon><Plus /></el-icon> 添加汇表</el-button>
            </div>
        </template>
        <div v-if="sinkTables.length === 0" style="text-align: center; padding: 40px; color: #999;">暂无汇表，点击右上角添加</div>
        <div v-if="sinkTables.length > 0">
            <el-card shadow="never" style="margin-bottom: 15px; background: #f8f9fa;">
                <template #header>
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-weight: 500;"><el-icon><Setting /></el-icon> 公共数据库连接配置（所有汇表共享）</span>
                        <el-button size="small" text type="primary" @click="applyCommonConfigToAllSinks">应用到所有表</el-button>
                    </div>
                </template>
                <el-form :model="sinkCommonConfig" label-width="100px" size="small">
                    <el-row :gutter="20">
                        <el-col :span="8">
                            <el-form-item label="连接器类型">
                                <el-select v-model="sinkCommonConfig.connectorType" placeholder="选择连接器" style="width: 100%;" filterable @change="handleCommonConnectorChange">
                                    <el-option-group v-for="group in sinkConnectorGroups" :key="group.label" :label="group.label">
                                        <el-option v-for="option in group.options" :key="option.value" :label="option.label" :value="option.value" />
                                    </el-option-group>
                                </el-select>
                            </el-form-item>
                        </el-col>
                        <el-col :span="8">
                            <el-form-item label="选择数据源">
                                <el-select v-model="sinkCommonConfig.datasourceId" placeholder="选择已保存的数据源" clearable @change="applyCommonDatasource" style="width: 100%">
                                    <el-option v-for="ds in getDatasourcesForConnector(sinkCommonConfig.connectorType)" :key="ds.id" :label="ds.name" :value="ds.id">
                                        <span>{{ ds.name }}</span>
                                        <span style="float: right; color: #8492a6; font-size: 13px">{{ ds.type }}</span>
                                    </el-option>
                                </el-select>
                            </el-form-item>
                        </el-col>
                    </el-row>

                    <div v-if="['jdbc', 'jdbc-upsert', 'postgres-jdbc', 'oracle-jdbc', 'sqlserver-jdbc'].includes(sinkCommonConfig.connectorType)">
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="JDBC URL"><el-input v-model="sinkCommonConfig.jdbcUrl" placeholder="jdbc:mysql://localhost:3306/db" @input="autoFillDriverCommon" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="驱动类"><el-input v-model="sinkCommonConfig.jdbcDriver" placeholder="com.mysql.cj.jdbc.Driver" /></el-form-item></el-col>
                        </el-row>
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="用户名"><el-input v-model="sinkCommonConfig.jdbcUsername" placeholder="用户名" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="密码"><el-input v-model="sinkCommonConfig.jdbcPassword" type="password" placeholder="密码" /></el-form-item></el-col>
                        </el-row>
                    </div>

                    <div v-if="sinkCommonConfig.connectorType === 'doris'">
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="FE 地址"><el-input v-model="sinkCommonConfig.dorisUrl" placeholder="jdbc:mysql://localhost:9030/db" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="驱动类"><el-input v-model="sinkCommonConfig.dorisDriver" placeholder="com.mysql.cj.jdbc.Driver" /></el-form-item></el-col>
                        </el-row>
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="用户名"><el-input v-model="sinkCommonConfig.dorisUsername" placeholder="用户名" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="密码"><el-input v-model="sinkCommonConfig.dorisPassword" type="password" placeholder="密码" /></el-form-item></el-col>
                        </el-row>
                    </div>

                    <div v-if="['jdbc', 'jdbc-upsert', 'postgres-jdbc', 'oracle-jdbc', 'sqlserver-jdbc'].includes(sinkCommonConfig.connectorType)">
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="JDBC URL"><el-input v-model="sinkCommonConfig.jdbcUrl" placeholder="jdbc:mysql://localhost:3306/db" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="驱动类"><el-input v-model="sinkCommonConfig.jdbcDriver" placeholder="com.mysql.cj.jdbc.Driver" /></el-form-item></el-col>
                        </el-row>
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="用户名"><el-input v-model="sinkCommonConfig.jdbcUsername" placeholder="用户名" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="密码"><el-input v-model="sinkCommonConfig.jdbcPassword" type="password" placeholder="密码" /></el-form-item></el-col>
                        </el-row>
                    </div>

                    <div v-if="sinkCommonConfig.connectorType === 'starrocks'">
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="FE 地址"><el-input v-model="sinkCommonConfig.starrocksUrl" placeholder="jdbc:mysql://localhost:9030/db" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="驱动类"><el-input v-model="sinkCommonConfig.starrocksDriver" placeholder="com.mysql.cj.jdbc.Driver" /></el-form-item></el-col>
                        </el-row>
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="用户名"><el-input v-model="sinkCommonConfig.starrocksUsername" placeholder="用户名" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="密码"><el-input v-model="sinkCommonConfig.starrocksPassword" type="password" placeholder="密码" /></el-form-item></el-col>
                        </el-row>
                    </div>

                    <div v-if="sinkCommonConfig.connectorType === 'clickhouse'">
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="URL"><el-input v-model="sinkCommonConfig.clickhouseUrl" placeholder="jdbc:clickhouse://localhost:8123/default" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="驱动类"><el-input v-model="sinkCommonConfig.clickhouseDriver" placeholder="com.clickhouse.jdbc.ClickHouseDriver" /></el-form-item></el-col>
                        </el-row>
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="用户名"><el-input v-model="sinkCommonConfig.clickhouseUsername" placeholder="default" /></el-form-item></el-col>
                            <el-col :span="12"><el-form-item label="密码"><el-input v-model="sinkCommonConfig.clickhousePassword" type="password" placeholder="密码" /></el-form-item></el-col>
                        </el-row>
                    </div>

                    <div v-if="sinkCommonConfig.connectorType === 'kafka'">
                        <el-row :gutter="20">
                            <el-col :span="12"><el-form-item label="Bootstrap Servers"><el-input v-model="sinkCommonConfig.kafkaBootstrapServers" placeholder="localhost:9092" /></el-form-item></el-col>
                            <el-col :span="6"><el-form-item label="消息格式"><el-select v-model="sinkCommonConfig.kafkaFormat" style="width: 100%;"><el-option label="JSON" value="json" /><el-option label="Avro" value="avro" /><el-option label="CSV" value="csv" /><el-option label="Canal JSON" value="canal-json" /><el-option label="Debezium JSON" value="debezium-json" /></el-select></el-form-item></el-col>
                            <el-col :span="6"><el-form-item label="语义保证"><el-select v-model="sinkCommonConfig.kafkaSemantic" style="width: 100%;"><el-option label="至少一次" value="at-least-once" /><el-option label="精确一次" value="exactly-once" /><el-option label="无保证" value="none" /></el-select></el-form-item></el-col>
                        </el-row>
                    </div>

                    <el-divider content-position="left">自动建表配置（所有汇表共享）</el-divider>
                    <el-row :gutter="20">
                        <el-col :span="6">
                            <el-form-item label="启用自动建表">
                                <el-switch v-model="sinkCommonConfig.autoCreateTable" />
                            </el-form-item>
                        </el-col>
                        <el-col :span="6" v-if="sinkCommonConfig.autoCreateTable && sinkTables.length > 0">
                            <el-form-item label="&nbsp;">
                                <el-button size="small" type="primary" plain @click="previewCreateTableSql(sinkTables[parseInt(currentSinkTabIndex)])"><el-icon><View /></el-icon> 预览建表语句</el-button>
                            </el-form-item>
                        </el-col>
                        <el-col :span="6" v-if="sinkCommonConfig.autoCreateTable && ['doris', 'starrocks'].includes(sinkCommonConfig.connectorType)">
                            <el-form-item label="建表类型">
                                <el-select v-model="sinkCommonConfig.tableType" style="width: 100%">
                                    <el-option value="UNIQUE" label="主键表(UNIQUE)" />
                                    <el-option value="AGGREGATE" label="聚合表(AGGREGATE)" />
                                    <el-option value="DUPLICATE" label="明细(DUPLICATE)" />
                                </el-select>
                            </el-form-item>
                        </el-col>
                        <el-col :span="6" v-if="sinkCommonConfig.autoCreateTable && ['doris', 'starrocks'].includes(sinkCommonConfig.connectorType)">
                            <el-form-item label="副本数量">
                                <el-input-number v-model="sinkCommonConfig.replicationNum" :min="1" :max="10" style="width: 100%" />
                            </el-form-item>
                        </el-col>
                    </el-row>
                    <el-row :gutter="20" v-if="sinkCommonConfig.autoCreateTable && ['doris', 'starrocks'].includes(sinkCommonConfig.connectorType)">
                        <el-col :span="6">
                            <el-form-item label="Buckets数量">
                                <el-input-number v-model="sinkCommonConfig.dorisBuckets" :min="1" :max="128" style="width: 100%" />
                            </el-form-item>
                        </el-col>
                    </el-row>

                    <el-divider content-position="left">攒批配置（所有汇表共享）</el-divider>
                    <el-row :gutter="20">
                        <el-col :span="6">
                            <el-form-item label="启用攒批">
                                <el-switch v-model="sinkCommonConfig.enableBatch" />
                            </el-form-item>
                        </el-col>
                        <el-col :span="6" v-if="sinkCommonConfig.enableBatch">
                            <el-form-item label="批次大小">
                                <el-input-number v-model="sinkCommonConfig.batchSize" :min="100" :max="100000" :step="100" style="width: 100%" />
                            </el-form-item>
                        </el-col>
                        <el-col :span="6" v-if="sinkCommonConfig.enableBatch">
                            <el-form-item label="Flush间隔(ms)">
                                <el-input-number v-model="sinkCommonConfig.batchInterval" :min="1000" :max="300000" :step="1000" style="width: 100%" />
                            </el-form-item>
                        </el-col>
                    </el-row>
                </el-form>
            </el-card>

            <el-tabs v-model="currentSinkTabIndex" type="card" closable @tab-remove="removeSinkTable">
                <el-tab-pane v-for="(table, index) in sinkTables" :key="index" :name="String(index)">
                    <template #label>
                        <span :title="table.tableName || '未命名汇表'">{{ table.tableName || '未命名汇表' }}</span>
                    </template>
                    <el-form :model="table" label-width="120px" size="small">
                        <el-row :gutter="20">
                            <el-col :span="12">
                                <el-form-item label="Flink表名">
                                    <el-input v-model="table.tableName" placeholder="表名" />
                                </el-form-item>
                            </el-col>
                            <el-col :span="12">
                                <el-form-item label="物理表名">
                                    <el-input v-model="table.physicalTableName" placeholder="实际写入的表名" />
                                </el-form-item>
                            </el-col>
                        </el-row>

                        <div v-if="table.connectorType === 'kafka'">
                            <el-row :gutter="20">
                                <el-col :span="12"><el-form-item label="Topic"><el-input v-model="table.kafkaTopic" placeholder="Kafka topic" /></el-form-item></el-col>
                            </el-row>
                        </div>

                    <el-divider content-position="left">字段配置</el-divider>
                    <div style="margin-bottom: 10px;">
                        <el-button size="small" @click="addField(table)"><el-icon><Plus /></el-icon> 添加字段</el-button>
                        <el-button size="small" @click="refreshSinkTableFields(table)"><el-icon><Refresh /></el-icon> 刷新表结构</el-button>
                    </div>
                    <div v-for="(field, fIndex) in table.fields" :key="fIndex" style="margin-bottom: 10px;">
                        <el-row :gutter="10">
                            <el-col :span="6"><el-input v-model="field.name" placeholder="字段名" size="small" /></el-col>
                            <el-col :span="5">
                                <select v-model="field.type" class="native-select">
                                    <option v-for="t in fieldTypes" :key="t" :value="t">{{ t }}</option>
                                </select>
                            </el-col>
                            <el-col :span="4"><el-input v-if="['VARCHAR','CHAR','DECIMAL','TIMESTAMP','TIMESTAMP_LTZ'].includes(field.type)" v-model="field.precision" placeholder="精度" size="small" /><span v-else style="color: #ccc;">-</span></el-col>
                            <el-col :span="4"><el-input v-if="field.type === 'DECIMAL'" v-model="field.scale" placeholder="标度" size="small" /><span v-else style="color: #ccc;">-</span></el-col>
                            <el-col :span="3"><el-checkbox v-model="field.primaryKey" @change="handlePrimaryKey(table, fIndex)">主键</el-checkbox></el-col>
                            <el-col :span="2"><el-button type="danger" link @click="removeField(table, fIndex)" size="small"><el-icon><Delete /></el-icon></el-button></el-col>
                        </el-row>
                    </div>
                    <div v-if="table.fields.length === 0" style="text-align: center; color: #999; padding: 20px; font-size: 13px;">暂无字段，请点击上方"添加字段"按钮</div>
                </el-form>
            </el-tab-pane>
        </el-tabs>
    </el-card>

    <el-card style="margin-top: 20px;">
        <template #header>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <span>查询配置</span>
                <div>
                    <el-button type="primary" size="small" @click="addQuery"><el-icon><Plus /></el-icon> 添加查询</el-button>
                    <el-button size="small" @click="clearAllQueries">清空所有</el-button>
                </div>
            </div>
        </template>
        <div v-if="queries.length === 0" style="text-align: center; padding: 40px; color: #999;">
            <div>
                <el-icon><InfoFilled /></el-icon>
                <p>暂无查询，请添加查询</p>
                <div style="margin-top: 15px; color: #666; font-size: 14px;">
                    <p>提示：</p>
                    <ul style="text-align: left; display: inline-block;">
                        <li>1. 先在"源表配置"中添加表</li>
                        <li>2. 再在"汇表配置"中添加表</li>
                        <li>3. 最后点击"添加查询"</li>
                    </ul>
                </div>
            </div>
        </div>
        <el-collapse v-if="queries.length > 0">
            <el-collapse-item v-for="(query, index) in queries" :key="index" :name="index">
                <template #title>
                    <div style="display: flex; align-items: center; gap: 10px; width: 100%;">
                        <el-icon><DocumentCopy /></el-icon>
                        <span>查询 {{ index + 1 }}: {{ query.sinkTable }}</span>
                        <div style="margin-left: auto;"><el-button size="small" type="danger" @click.stop="removeQuery(index)">删除</el-button></div>
                    </div>
                </template>
                <el-form :model="query" label-width="120px" size="small">
                    <el-row :gutter="20">
                        <el-col :span="12">
                            <el-form-item label="汇表">
                                <el-select v-model="query.sinkTable" placeholder="选择汇表" style="width: 100%;">
                                    <el-option v-for="table in sinkTables" :key="table.tableName" :label="table.tableName" :value="table.tableName" />
                                </el-select>
                            </el-form-item>
                        </el-col>
                        <el-col :span="12">
                            <el-form-item label="源表">
                                <el-select v-model="query.sourceTable" placeholder="选择源表" style="width: 100%;" @change="onQuerySourceTableChange(query, $event)">
                                    <el-option v-for="table in sourceTables" :key="table.tableName" :label="table.tableName" :value="table.tableName" />
                                </el-select>
                            </el-form-item>
                        </el-col>
                    </el-row>

                    <el-form-item label="查询类型">
                        <el-radio-group v-model="query.queryType" @change="onQueryTypeChange(query)">
                            <el-radio label="insert">INSERT 查询</el-radio>
                            <el-radio label="aggregate">聚合查询</el-radio>
                            <el-radio label="join">JOIN 查询</el-radio>
                            <el-radio label="window">窗口查询</el-radio>
                            <el-radio label="custom">自定义SQL</el-radio>
                        </el-radio-group>
                    </el-form-item>

                    <div v-if="query.queryType === 'insert'">
                        <el-form-item label="选择字段">
                            <el-checkbox-group v-model="query.selectFields">
                                <el-checkbox v-for="field in getFieldsByTable(query.sourceTable)" :key="field.name" :label="field.name">{{ field.name }} ({{ field.type }})</el-checkbox>
                            </el-checkbox-group>
                        </el-form-item>
                        <el-form-item label="WHERE 条件">
                            <el-input v-model="query.whereClause" type="textarea" :rows="2" placeholder="例如: id > 100 AND name LIKE '%test%'" />
                        </el-form-item>
                    </div>

                    <div v-if="query.queryType === 'aggregate'">
                        <el-form-item label="分组字段">
                            <el-checkbox-group v-model="query.groupByFields">
                                <el-checkbox v-for="field in getFieldsByTable(query.sourceTable)" :key="field.name" :label="field.name">{{ field.name }} ({{ field.type }})</el-checkbox>
                            </el-checkbox-group>
                        </el-form-item>
                        <el-form-item label="聚合函数">
                            <div v-for="(agg, aggIndex) in query.aggregates" :key="aggIndex" style="margin-bottom: 10px;">
                                <el-row :gutter="10">
                                    <el-col :span="6">
                                        <el-select v-model="agg.function" placeholder="函数" size="small" style="width: 100%;">
                                            <el-option label="COUNT" value="COUNT" /><el-option label="SUM" value="SUM" />
                                            <el-option label="AVG" value="AVG" /><el-option label="MIN" value="MIN" />
                                            <el-option label="MAX" value="MAX" />
                                        </el-select>
                                    </el-col>
                                    <el-col :span="6">
                                        <el-select v-model="agg.field" placeholder="字段" size="small" style="width: 100%;">
                                            <el-option v-for="field in getFieldsByTable(query.sourceTable)" :key="field.name" :label="field.name" :value="field.name" />
                                        </el-select>
                                    </el-col>
                                    <el-col :span="10"><el-input v-model="agg.alias" placeholder="别名 (可选)" size="small" /></el-col>
                                    <el-col :span="2"><el-button size="small" type="danger" @click="removeAggregate(query, aggIndex)">删除</el-button></el-col>
                                </el-row>
                            </div>
                            <el-button size="small" @click="addAggregate(query)">添加聚合</el-button>
                        </el-form-item>
                        <el-form-item label="HAVING 条件">
                            <el-input v-model="query.havingClause" type="textarea" :rows="2" placeholder="例如: COUNT(*) > 10" />
                        </el-form-item>
                    </div>

                    <div v-if="query.queryType === 'join'">
                        <el-form-item label="连接类型">
                            <el-select v-model="query.joinType" placeholder="选择连接类型" style="width: 100%;">
                                <el-option label="INNER JOIN" value="INNER" /><el-option label="LEFT JOIN" value="LEFT" />
                                <el-option label="RIGHT JOIN" value="RIGHT" /><el-option label="FULL JOIN" value="FULL" />
                            </el-select>
                        </el-form-item>
                        <el-form-item label="连接表">
                            <el-select v-model="query.joinTable" placeholder="选择连接表" style="width: 100%;">
                                <el-option v-for="table in getOtherSourceTables(query.sourceTable)" :key="table.tableName" :label="table.tableName" :value="table.tableName" />
                            </el-select>
                        </el-form-item>
                        <el-form-item label="连接条件">
                            <el-input v-model="query.joinCondition" placeholder="例如: t1.id = t2.user_id" />
                        </el-form-item>
                    </div>

                    <div v-if="query.queryType === 'window'">
                        <el-row :gutter="20">
                            <el-col :span="8">
                                <el-form-item label="窗口类型">
                                    <el-select v-model="query.windowType" placeholder="选择窗口类型" style="width: 100%;">
                                        <el-option label="滚动窗口 (TUMBLE)" value="TUMBLE" />
                                        <el-option label="滑动窗口 (HOP)" value="HOP" />
                                        <el-option label="会话窗口 (SESSION)" value="SESSION" />
                                    </el-select>
                                </el-form-item>
                            </el-col>
                            <el-col :span="8">
                                <el-form-item label="窗口大小">
                                    <el-input v-model="query.windowSize" placeholder="5 MINUTES" />
                                </el-form-item>
                            </el-col>
                            <el-col :span="8" v-if="query.windowType === 'HOP'">
                                <el-form-item label="滑动步长">
                                    <el-input v-model="query.slideSize" placeholder="1 MINUTE" />
                                </el-form-item>
                            </el-col>
                        </el-row>
                        <el-form-item label="时间字段">
                            <el-select v-model="query.windowTimeField" placeholder="选择时间字段" style="width: 100%;">
                                <el-option v-for="field in getTimeFields(getFieldsByTable(query.sourceTable))" :key="field.name" :label="field.name" :value="field.name" />
                            </el-select>
                        </el-form-item>
                    </div>

                    <div v-if="query.queryType === 'custom'">
                        <el-form-item label="自定义SQL">
                            <el-input v-model="query.customSql" type="textarea" :rows="6" placeholder="输入完整的SQL语句..." />
                        </el-form-item>
                    </div>
                </el-form>
            </el-collapse-item>
        </el-collapse>
    </el-card>
</div>

<el-dialog v-model="sqlPreviewVisible" title="SQL 预览" width="800px">
    <pre style="white-space: pre-wrap; word-break: break-all; max-height: 500px; overflow-y: auto; background-color: #f5f7fa; padding: 15px; border-radius: 4px; border: 1px solid #dcdfe6; font-family: Consolas, Monaco, monospace; font-size: 14px;">{{ generatedSql }}</pre>
    <template #footer>
        <el-button @click="sqlPreviewVisible = false">关闭</el-button>
        <el-button type="primary" @click="copySql"><el-icon><DocumentCopy /></el-icon> 复制SQL</el-button>
        <el-button type="success" @click="submitSql" :loading="submitting"><el-icon><VideoPlay /></el-icon> 提交运行</el-button>
    </template>
</el-dialog>

<el-dialog v-model="tableSelectionVisible" title="选择表（可多选）" width="600px">
    <div v-if="loadingTables" style="text-align: center; padding: 40px;"><el-icon class="is-loading"><Loading /></el-icon> 加载中...</div>
    <div v-else-if="tableList.length === 0" style="text-align: center; padding: 40px; color: #999;">暂无表数据</div>
    <el-table v-else :data="tableList" style="width: 100%;" row-key="table_name" @selection-change="handleTableSelectionChange">
        <el-table-column type="selection" width="55" />
        <el-table-column prop="table_name" label="表名" min-width="200" />
        <el-table-column prop="table_type" label="类型" width="120" />
    </el-table>
    <template #footer>
        <el-button @click="tableSelectionVisible = false">取消</el-button>
        <el-button type="primary" @click="confirmTableSelection" :disabled="selectedTables.length === 0">确定（已选{{ selectedTables.length }}个）</el-button>
    </template>
</el-dialog>

<el-dialog v-model="createTableSqlVisible" title="建表语句预览" width="800px">
    <div style="margin-bottom: 10px; color: #909399; font-size: 13px;">您可以修改建表语句后执行建表操作：</div>
    <el-input v-model="currentCreateTableSql" type="textarea" :rows="15" style="font-family: Consolas, Monaco, monospace;" />
    <template #footer>
        <el-button @click="createTableSqlVisible = false">关闭</el-button>
        <el-button type="primary" @click="executeCreateTable"><el-icon><VideoPlay /></el-icon> 执行建表</el-button>
    </template>
</el-dialog>
    `
};