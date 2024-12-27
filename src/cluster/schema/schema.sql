CREATE TABLE IF NOT EXISTS nodes
(
    id
                 varchar(100) NOT NULL COMMENT 'uuid,节点唯一标识',
    ip           varchar(100) NOT NULL COMMENT 'IP地址',
    port         INT          NOT NULL COMMENT '服务端口',
    state        INT(11)      NULL COMMENT '节点状态',
    metric       TEXT         NULL COMMENT '节点指标信息',
    node_config  TEXT         NULL COMMENT '节点配置',
    region       varchar(100) NULL COMMENT '地域信息',
    zone         varchar(100) NULL COMMENT '可用区信息',
    `group`      TEXT         NULL COMMENT '组',
    gmt_create   datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    gmt_modified datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    incarnation  BIGINT COMMENT '元信息版本',
    last_modify  BIGINT COMMENT '最新修改时间，心跳时间',
    PRIMARY KEY
        (
         `id`
            )
);

CREATE TABLE IF NOT EXISTS binlog_instances
(
    node_id
                        varchar(100) NOT NULL COMMENT 'OBM的节点ID',
    ip                  VARCHAR(255) NOT NULL COMMENT 'OBI的服务IP',
    instance_name       VARCHAR(64)  NOT NULL COMMENT 'OBI 名称',
    port                INT(11)      NOT NULL DEFAULT 0 COMMENT 'OBI的服务port',
    cluster             VARCHAR(200) NOT NULL COMMENT 'OBI服务的集群ID',
    tenant              VARCHAR(200) NOT NULL COMMENT 'OBI服务的租户ID',
    pid                 INT(11) COMMENT 'OBI 进程ID',
    work_path           VARCHAR(200) NOT NULL DEFAULT '' COMMENT 'OBI的工作目录',
    state               INT(11)      NOT NULL COMMENT 'OBI状态，INIT、RUNNING、STOP、OFFLINE',
    config              TEXT COMMENT 'OBI的配置',
    gmt_create          datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    gmt_modified        datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    heartbeat           bigint(20)            DEFAULT '0' COMMENT '心跳时间',
    delay               bigint(20)   NOT NULL DEFAULT -1 COMMENT '延迟时间',
    min_dump_checkpoint bigint(20)            DEFAULT '0' COMMENT '最早订阅位点',
    PRIMARY KEY
        (
         instance_name
            ),
    INDEX obi_list
        (
         cluster,
         tenant
            )
);

CREATE TABLE IF NOT EXISTS `config_template`
(
    `id`
                  bigint
                      UNSIGNED
                               NOT
                                   NULL
        AUTO_INCREMENT
        COMMENT
            'PK',
    `version`
                  varchar(32)           DEFAULT '1' COMMENT '@desc 配置的版本',
    `key_name`    varchar(50)  NOT NULL COMMENT '@desc key',
    `value`       text         NOT NULL COMMENT '@desc 值',
    `granularity` int UNSIGNED NOT NULL DEFAULT '0',
    `scope`       varchar(128)          DEFAULT '' COMMENT '@desc 作用范围',
    gmt_create    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    gmt_modified  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY
        (
         `id`
            ),
    CONSTRAINT `unique_config` UNIQUE
        (
         `key_name`,
         `granularity`,
         `scope`
            )
) DEFAULT CHARSET = utf8 COMMENT ='config表';

CREATE TABLE IF NOT EXISTS `tasks`
(
    `id`
                     int(11)  NOT NULL AUTO_INCREMENT COMMENT 'PK',
    `task_id`        text     NOT NULL COMMENT 'task id',
    `type`           int(11)           DEFAULT NULL COMMENT 'task 类型',
    `instance_name`  text              DEFAULT NULL COMMENT 'instance 名称',
    `status`         int(11)  NOT NULL COMMENT 'task 状态',
    `execution_node` text     NOT NULL COMMENT '执行 task 的节点',
    `retry_count`    int(11)           DEFAULT '0' COMMENT 'task 执行最大重试次数',
    `last_modify`    bigint   NOT NULL DEFAULT 0 COMMENT 'task 最后更新时间',
    `task_param`     text              DEFAULT NULL COMMENT 'task 参数',
    gmt_create       datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    gmt_modified     datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY
        (
         `id`
            )
) DEFAULT CHARSET = utf8 COMMENT ='异步任务表';

CREATE TABLE IF NOT EXISTS `primary_instance`
(
    cluster
                    VARCHAR(200) NOT NULL COMMENT 'OBI服务的集群ID',
    tenant          VARCHAR(200) NOT NULL COMMENT 'OBI服务的租户ID',
    master_instance VARCHAR(255) NOT NULL COMMENT '当前租户的主 instance',
    gmt_create      datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    gmt_modified    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY
        (
         `cluster`,
         `tenant`
            )
);

CREATE TABLE IF NOT EXISTS `instances_gtid_seq`
(
    `id`
                           BIGINT
                               UNSIGNED
                                            NOT
                                                NULL
        AUTO_INCREMENT
        COMMENT
            'PK',
    `cluster`
                           VARCHAR(200)     NOT NULL COMMENT 'OBI服务的集群ID',
    `tenant`               VARCHAR(200)     NOT NULL COMMENT 'OBI服务的租户ID',
    `instance`             VARCHAR(64)      NOT NULL COMMENT 'OBI 名称',
    `commit_version_start` BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '事务提交时间',
    `xid_start`            VARCHAR(255)     NOT NULL COMMENT '事务 ID',
    `gtid_start`           BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '事务对应的 gtid',
    `trxs_num`             INT(11) UNSIGNED NOT NULL DEFAULT 0 COMMENT '压缩事务的数量',
    gmt_create             datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    gmt_modified           datetime         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    INDEX
        (
         `instance`,
         `commit_version_start`
            ),
    PRIMARY KEY
        (
         `id`
            )
);

CREATE TABLE IF NOT EXISTS `user`
(
    `id`
                    int(11)      NOT NULL AUTO_INCREMENT COMMENT 'PK',
    `username`      varchar(128) NOT NULL COMMENT '用户名',
    `password_sha1` varchar(128) NOT NULL DEFAULT '' COMMENT 'SHA1(用户密码)',
    `password`      varchar(128) NOT NULL DEFAULT '' COMMENT 'SHA1(SHA1(用户密码))',
    `tag`           int(4)       NOT NULL COMMENT '标签，0: 固定账密, 1: 用户可修改',
    `gmt_create`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `gmt_modified`  datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
    PRIMARY KEY
        (
         `id`
            )
) DEFAULT CHARSET = utf8 COMMENT ='用户表';

## DML变更
REPLACE INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_expire_logs_seconds', '259200', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_expire_logs_size', '53687091200', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'auto_start_obcdc', '1', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'active_state_after_boot', '0', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'failover', '1', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_gtid_display', 'true', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_ddl_convert', 'true', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_memory_limit', '4G', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_working_mode', 'storage', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_recover_backup', 'true', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'gtid_seq_compressed_interval_s', '10', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'gtid_seq_compressed_trx_size', '100000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'gtid_marking_step_size', '100000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'gtid_inspector_s', '900', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'gtid_memory_cache_seconds', '7200', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'gtid_heartbeat_duration_s', '3600', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'recovery_point_strategy', 'fast', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'max_binlog_size', '536870912', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_log_heartbeat_interval_times', '10', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'verbose_record_read', 'false', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_timezone_conf', '../../conf/timezone_info.conf', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_obcdc_path_template', '../../obcdc/obcdc-%s.x-access/libobcdcaccess.so', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_obcdc_ce_path_template', '../../obcdc/obcdc-ce-%s.x-access/libobcdcaccess.so', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_ignore_unsupported_event', 'true', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_max_event_buffer_bytes', '67108864', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_convert_timeout_us', '10000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_ddl_convert_jvm_options',
        '-Djava.class.path=../../deps/lib/etransfer.jar|-Xmx256M|-Xtrace|-XX:+CreateMinidumpOnCrash', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_ddl_convert_class', 'com/alipay/oms/etransfer/util/OB2MySQLConvertTool', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_ddl_convert_func', 'parser', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_ddl_convert_func_param', '(Ljava/lang/String;Ljava/lang/String;Z)Ljava/lang/String;', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'enable_resource_check', 'true', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'node_cpu_limit_threshold_percent', '80', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'node_mem_limit_threshold_percent', '85', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'node_disk_limit_threshold_percent', '70', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'enable_dumper_interception', 'false', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'max_dumper_num', '128', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'default_instance_replicate_num', '1', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'max_instance_replicate_num', '5', 0, '');


REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'max_task_execution_time_s', '600', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'default_defer_drop_sec', '0', 0, '');

-- v3.2.0 引入
REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_ddl_convert_ignore_unsupported_ddl', 'true', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'prometheus_unused_metric_clear_interval_s', '900', 0, '');

-- v3.2.1 引入
REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'enable_auth', 'false', 0, '');

REPLACE
    INTO `user` (`username`, `tag`)
VALUES ('sys', '0');
REPLACE
    INTO `user` (`username`, `tag`)
VALUES ('admin', '1');

-- v4.2.0 引入
REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'record_queue_size', '200000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'read_wait_num', '20000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'storage_wait_num', '100000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'read_timeout_us', '10000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'storage_timeout_us', '10000', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_convert_ring_buffer_size', '1024', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_convert_number_of_concurrences', '12', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_convert_thread_size', '16', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_serialize_ring_buffer_size', '8', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_serialize_thread_size', '10', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_serialize_parallel_size', '8', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_release_ring_buffer_size', '1024', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_release_thread_size', '4', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'binlog_release_parallel_size', '2', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'preallocated_memory_bytes', '2097152', 0, '');

REPLACE
    INTO `config_template` (`version`, `key_name`, `value`, `granularity`, `scope`)
VALUES ('1', 'preallocated_expansion_memory_bytes', '8192', 0, '');

