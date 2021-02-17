spark_config = {'spark.sql.shuffle.partitions': '960', 
                'spark.default.parallelism': '480',
                'spark.app.name': 'spotify',
                'yarn.nodemanager.resource.memory-mb' : '30g',
                'yarn.nodemanager.resource.cpu-vcores' : '15',
                'spark.executor.instances' : '48',
                'spark.executor.cores' : '5',
                'spark.executor.memory' : '10240m',
                'spark.executor.memoryOverhead' : '5120m',
                'spark.driver.memory' : '10240m',
                'spark.driver.memoryOverhead' : '5120m',
                'spark.driver.maxResultSize' : '10g',
                'spark.network.timeout' : '1200',
                'spark.yarn.am.memory' : '10240m',
                'spark.yarn.am.cores' : '5',
                'spark.yarn.am.memoryOverhead' : '5120m',
                }


hdfs_path = 'hdfs://dp-01.tap-psnc.net:9000/user/dpuser'