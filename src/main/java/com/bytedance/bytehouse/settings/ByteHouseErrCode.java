/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bytedance.bytehouse.settings;

import java.util.Locale;

/**
 * Error code from the server.
 * <a href="https://code.byted.org/dp/ClickHouse/blob/cnch_dev/dbms/src/Common/ErrorCodes.cpp">ErrorCodes.cpp</a>
 */
public enum ByteHouseErrCode {
    UNKNOWN_ERROR(-2, "UNKNOWN_ERROR"),
    CLIENT_ERROR(-1, "CLIENT_ERROR"),
    OK(0, "OK"),
    UNSUPPORTED_METHOD(1, "UNSUPPORTED_METHOD"),
    UNSUPPORTED_PARAMETER(2, "UNSUPPORTED_PARAMETER"),
    UNEXPECTED_END_OF_FILE(3, "UNEXPECTED_END_OF_FILE"),
    EXPECTED_END_OF_FILE(4, "EXPECTED_END_OF_FILE"),
    CANNOT_PARSE_TEXT(6, "CANNOT_PARSE_TEXT"),
    INCORRECT_NUMBER_OF_COLUMNS(7, "INCORRECT_NUMBER_OF_COLUMNS"),
    THERE_IS_NO_COLUMN(8, "THERE_IS_NO_COLUMN"),
    SIZES_OF_COLUMNS_DOESNT_MATCH(9, "SIZES_OF_COLUMNS_DOESNT_MATCH"),
    NOT_FOUND_COLUMN_IN_BLOCK(10, "NOT_FOUND_COLUMN_IN_BLOCK"),
    POSITION_OUT_OF_BOUND(11, "POSITION_OUT_OF_BOUND"),
    PARAMETER_OUT_OF_BOUND(12, "PARAMETER_OUT_OF_BOUND"),
    SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH(13, "SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH"),
    DUPLICATE_COLUMN(15, "DUPLICATE_COLUMN"),
    NO_SUCH_COLUMN_IN_TABLE(16, "NO_SUCH_COLUMN_IN_TABLE"),
    DELIMITER_IN_STRING_LITERAL_DOESNT_MATCH(17, "DELIMITER_IN_STRING_LITERAL_DOESNT_MATCH"),
    CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN(18, "CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN"),
    SIZE_OF_FIXED_STRING_DOESNT_MATCH(19, "SIZE_OF_FIXED_STRING_DOESNT_MATCH"),
    NUMBER_OF_COLUMNS_DOESNT_MATCH(20, "NUMBER_OF_COLUMNS_DOESNT_MATCH"),
    CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT(21, "CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT"),
    CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT(22, "CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT"),
    CANNOT_READ_FROM_ISTREAM(23, "CANNOT_READ_FROM_ISTREAM"),
    CANNOT_WRITE_TO_OSTREAM(24, "CANNOT_WRITE_TO_OSTREAM"),
    CANNOT_PARSE_ESCAPE_SEQUENCE(25, "CANNOT_PARSE_ESCAPE_SEQUENCE"),
    CANNOT_PARSE_QUOTED_STRING(26, "CANNOT_PARSE_QUOTED_STRING"),
    CANNOT_PARSE_INPUT_ASSERTION_FAILED(27, "CANNOT_PARSE_INPUT_ASSERTION_FAILED"),
    CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER(28, "CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER"),
    CANNOT_PRINT_INTEGER(29, "CANNOT_PRINT_INTEGER"),
    CANNOT_READ_SIZE_OF_COMPRESSED_CHUNK(30, "CANNOT_READ_SIZE_OF_COMPRESSED_CHUNK"),
    CANNOT_READ_COMPRESSED_CHUNK(31, "CANNOT_READ_COMPRESSED_CHUNK"),
    ATTEMPT_TO_READ_AFTER_EOF(32, "ATTEMPT_TO_READ_AFTER_EOF"),
    CANNOT_READ_ALL_DATA(33, "CANNOT_READ_ALL_DATA"),
    TOO_MANY_ARGUMENTS_FOR_FUNCTION(34, "TOO_MANY_ARGUMENTS_FOR_FUNCTION"),
    TOO_FEW_ARGUMENTS_FOR_FUNCTION(35, "TOO_FEW_ARGUMENTS_FOR_FUNCTION"),
    BAD_ARGUMENTS(36, "BAD_ARGUMENTS"),
    UNKNOWN_ELEMENT_IN_AST(37, "UNKNOWN_ELEMENT_IN_AST"),
    CANNOT_PARSE_DATE(38, "CANNOT_PARSE_DATE"),
    TOO_LARGE_SIZE_COMPRESSED(39, "TOO_LARGE_SIZE_COMPRESSED"),
    CHECKSUM_DOESNT_MATCH(40, "CHECKSUM_DOESNT_MATCH"),
    CANNOT_PARSE_DATETIME(41, "CANNOT_PARSE_DATETIME"),
    NUMBER_OF_ARGUMENTS_DOESNT_MATCH(42, "NUMBER_OF_ARGUMENTS_DOESNT_MATCH"),
    ILLEGAL_TYPE_OF_ARGUMENT(43, "ILLEGAL_TYPE_OF_ARGUMENT"),
    ILLEGAL_COLUMN(44, "ILLEGAL_COLUMN"),
    ILLEGAL_NUMBER_OF_RESULT_COLUMNS(45, "ILLEGAL_NUMBER_OF_RESULT_COLUMNS"),
    UNKNOWN_FUNCTION(46, "UNKNOWN_FUNCTION"),
    UNKNOWN_IDENTIFIER(47, "UNKNOWN_IDENTIFIER"),
    NOT_IMPLEMENTED(48, "NOT_IMPLEMENTED"),
    LOGICAL_ERROR(49, "LOGICAL_ERROR"),
    UNKNOWN_TYPE(50, "UNKNOWN_TYPE"),
    EMPTY_LIST_OF_COLUMNS_QUERIED(51, "EMPTY_LIST_OF_COLUMNS_QUERIED"),
    COLUMN_QUERIED_MORE_THAN_ONCE(52, "COLUMN_QUERIED_MORE_THAN_ONCE"),
    TYPE_MISMATCH(53, "TYPE_MISMATCH"),
    STORAGE_DOESNT_ALLOW_PARAMETERS(54, "STORAGE_DOESNT_ALLOW_PARAMETERS"),
    STORAGE_REQUIRES_PARAMETER(55, "STORAGE_REQUIRES_PARAMETER"),
    UNKNOWN_STORAGE(56, "UNKNOWN_STORAGE"),
    TABLE_ALREADY_EXISTS(57, "TABLE_ALREADY_EXISTS"),
    TABLE_METADATA_ALREADY_EXISTS(58, "TABLE_METADATA_ALREADY_EXISTS"),
    ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER(59, "ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER"),
    UNKNOWN_TABLE(60, "UNKNOWN_TABLE"),
    ONLY_FILTER_COLUMN_IN_BLOCK(61, "ONLY_FILTER_COLUMN_IN_BLOCK"),
    SYNTAX_ERROR(62, "SYNTAX_ERROR"),
    UNKNOWN_AGGREGATE_FUNCTION(63, "UNKNOWN_AGGREGATE_FUNCTION"),
    CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT(64, "CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT"),
    CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT(65, "CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT"),
    NOT_A_COLUMN(66, "NOT_A_COLUMN"),
    ILLEGAL_KEY_OF_AGGREGATION(67, "ILLEGAL_KEY_OF_AGGREGATION"),
    CANNOT_GET_SIZE_OF_FIELD(68, "CANNOT_GET_SIZE_OF_FIELD"),
    ARGUMENT_OUT_OF_BOUND(69, "ARGUMENT_OUT_OF_BOUND"),
    CANNOT_CONVERT_TYPE(70, "CANNOT_CONVERT_TYPE"),
    CANNOT_WRITE_AFTER_END_OF_BUFFER(71, "CANNOT_WRITE_AFTER_END_OF_BUFFER"),
    CANNOT_PARSE_NUMBER(72, "CANNOT_PARSE_NUMBER"),
    UNKNOWN_FORMAT(73, "UNKNOWN_FORMAT"),
    CANNOT_READ_FROM_FILE_DESCRIPTOR(74, "CANNOT_READ_FROM_FILE_DESCRIPTOR"),
    CANNOT_WRITE_TO_FILE_DESCRIPTOR(75, "CANNOT_WRITE_TO_FILE_DESCRIPTOR"),
    CANNOT_OPEN_FILE(76, "CANNOT_OPEN_FILE"),
    CANNOT_CLOSE_FILE(77, "CANNOT_CLOSE_FILE"),
    UNKNOWN_TYPE_OF_QUERY(78, "UNKNOWN_TYPE_OF_QUERY"),
    INCORRECT_FILE_NAME(79, "INCORRECT_FILE_NAME"),
    INCORRECT_QUERY(80, "INCORRECT_QUERY"),
    UNKNOWN_DATABASE(81, "UNKNOWN_DATABASE"),
    DATABASE_ALREADY_EXISTS(82, "DATABASE_ALREADY_EXISTS"),
    DIRECTORY_DOESNT_EXIST(83, "DIRECTORY_DOESNT_EXIST"),
    DIRECTORY_ALREADY_EXISTS(84, "DIRECTORY_ALREADY_EXISTS"),
    FORMAT_IS_NOT_SUITABLE_FOR_INPUT(85, "FORMAT_IS_NOT_SUITABLE_FOR_INPUT"),
    RECEIVED_ERROR_FROM_REMOTE_IO_SERVER(86, "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER"),
    CANNOT_SEEK_THROUGH_FILE(87, "CANNOT_SEEK_THROUGH_FILE"),
    CANNOT_TRUNCATE_FILE(88, "CANNOT_TRUNCATE_FILE"),
    UNKNOWN_COMPRESSION_METHOD(89, "UNKNOWN_COMPRESSION_METHOD"),
    EMPTY_LIST_OF_COLUMNS_PASSED(90, "EMPTY_LIST_OF_COLUMNS_PASSED"),
    SIZES_OF_MARKS_FILES_ARE_INCONSISTENT(91, "SIZES_OF_MARKS_FILES_ARE_INCONSISTENT"),
    EMPTY_DATA_PASSED(92, "EMPTY_DATA_PASSED"),
    UNKNOWN_AGGREGATED_DATA_VARIANT(93, "UNKNOWN_AGGREGATED_DATA_VARIANT"),
    CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS(94, "CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS"),
    CANNOT_READ_FROM_SOCKET(95, "CANNOT_READ_FROM_SOCKET"),
    CANNOT_WRITE_TO_SOCKET(96, "CANNOT_WRITE_TO_SOCKET"),
    CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT(97, "CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT"),
    CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM(98, "CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM"),
    UNKNOWN_PACKET_FROM_CLIENT(99, "UNKNOWN_PACKET_FROM_CLIENT"),
    UNKNOWN_PACKET_FROM_SERVER(100, "UNKNOWN_PACKET_FROM_SERVER"),
    UNEXPECTED_PACKET_FROM_CLIENT(101, "UNEXPECTED_PACKET_FROM_CLIENT"),
    UNEXPECTED_PACKET_FROM_SERVER(102, "UNEXPECTED_PACKET_FROM_SERVER"),
    RECEIVED_DATA_FOR_WRONG_QUERY_ID(103, "RECEIVED_DATA_FOR_WRONG_QUERY_ID"),
    TOO_SMALL_BUFFER_SIZE(104, "TOO_SMALL_BUFFER_SIZE"),
    CANNOT_READ_HISTORY(105, "CANNOT_READ_HISTORY"),
    CANNOT_APPEND_HISTORY(106, "CANNOT_APPEND_HISTORY"),
    FILE_DOESNT_EXIST(107, "FILE_DOESNT_EXIST"),
    NO_DATA_TO_INSERT(108, "NO_DATA_TO_INSERT"),
    CANNOT_BLOCK_SIGNAL(109, "CANNOT_BLOCK_SIGNAL"),
    CANNOT_UNBLOCK_SIGNAL(110, "CANNOT_UNBLOCK_SIGNAL"),
    CANNOT_MANIPULATE_SIGSET(111, "CANNOT_MANIPULATE_SIGSET"),
    CANNOT_WAIT_FOR_SIGNAL(112, "CANNOT_WAIT_FOR_SIGNAL"),
    THERE_IS_NO_SESSION(113, "THERE_IS_NO_SESSION"),
    CANNOT_CLOCK_GETTIME(114, "CANNOT_CLOCK_GETTIME"),
    UNKNOWN_SETTING(115, "UNKNOWN_SETTING"),
    THERE_IS_NO_DEFAULT_VALUE(116, "THERE_IS_NO_DEFAULT_VALUE"),
    INCORRECT_DATA(117, "INCORRECT_DATA"),
    ENGINE_REQUIRED(119, "ENGINE_REQUIRED"),
    CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE(120, "CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE"),
    UNSUPPORTED_JOIN_KEYS(121, "UNSUPPORTED_JOIN_KEYS"),
    INCOMPATIBLE_COLUMNS(122, "INCOMPATIBLE_COLUMNS"),
    UNKNOWN_TYPE_OF_AST_NODE(123, "UNKNOWN_TYPE_OF_AST_NODE"),
    INCORRECT_ELEMENT_OF_SET(124, "INCORRECT_ELEMENT_OF_SET"),
    INCORRECT_RESULT_OF_SCALAR_SUBQUERY(125, "INCORRECT_RESULT_OF_SCALAR_SUBQUERY"),
    CANNOT_GET_RETURN_TYPE(126, "CANNOT_GET_RETURN_TYPE"),
    ILLEGAL_INDEX(127, "ILLEGAL_INDEX"),
    TOO_LARGE_ARRAY_SIZE(128, "TOO_LARGE_ARRAY_SIZE"),
    FUNCTION_IS_SPECIAL(129, "FUNCTION_IS_SPECIAL"),
    CANNOT_READ_ARRAY_FROM_TEXT(130, "CANNOT_READ_ARRAY_FROM_TEXT"),
    TOO_LARGE_STRING_SIZE(131, "TOO_LARGE_STRING_SIZE"),
    AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS(133, "AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS"),
    PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS(134, "PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS"),
    ZERO_ARRAY_OR_TUPLE_INDEX(135, "ZERO_ARRAY_OR_TUPLE_INDEX"),
    UNKNOWN_ELEMENT_IN_CONFIG(137, "UNKNOWN_ELEMENT_IN_CONFIG"),
    EXCESSIVE_ELEMENT_IN_CONFIG(138, "EXCESSIVE_ELEMENT_IN_CONFIG"),
    NO_ELEMENTS_IN_CONFIG(139, "NO_ELEMENTS_IN_CONFIG"),
    ALL_REQUESTED_COLUMNS_ARE_MISSING(140, "ALL_REQUESTED_COLUMNS_ARE_MISSING"),
    SAMPLING_NOT_SUPPORTED(141, "SAMPLING_NOT_SUPPORTED"),
    NOT_FOUND_NODE(142, "NOT_FOUND_NODE"),
    FOUND_MORE_THAN_ONE_NODE(143, "FOUND_MORE_THAN_ONE_NODE"),
    FIRST_DATE_IS_BIGGER_THAN_LAST_DATE(144, "FIRST_DATE_IS_BIGGER_THAN_LAST_DATE"),
    UNKNOWN_OVERFLOW_MODE(145, "UNKNOWN_OVERFLOW_MODE"),
    QUERY_SECTION_DOESNT_MAKE_SENSE(146, "QUERY_SECTION_DOESNT_MAKE_SENSE"),
    NOT_FOUND_FUNCTION_ELEMENT_FOR_AGGREGATE(147, "NOT_FOUND_FUNCTION_ELEMENT_FOR_AGGREGATE"),
    NOT_FOUND_RELATION_ELEMENT_FOR_CONDITION(148, "NOT_FOUND_RELATION_ELEMENT_FOR_CONDITION"),
    NOT_FOUND_RHS_ELEMENT_FOR_CONDITION(149, "NOT_FOUND_RHS_ELEMENT_FOR_CONDITION"),
    EMPTY_LIST_OF_ATTRIBUTES_PASSED(150, "EMPTY_LIST_OF_ATTRIBUTES_PASSED"),
    INDEX_OF_COLUMN_IN_SORT_CLAUSE_IS_OUT_OF_RANGE(151, "INDEX_OF_COLUMN_IN_SORT_CLAUSE_IS_OUT_OF_RANGE"),
    UNKNOWN_DIRECTION_OF_SORTING(152, "UNKNOWN_DIRECTION_OF_SORTING"),
    ILLEGAL_DIVISION(153, "ILLEGAL_DIVISION"),
    AGGREGATE_FUNCTION_NOT_APPLICABLE(154, "AGGREGATE_FUNCTION_NOT_APPLICABLE"),
    UNKNOWN_RELATION(155, "UNKNOWN_RELATION"),
    DICTIONARIES_WAS_NOT_LOADED(156, "DICTIONARIES_WAS_NOT_LOADED"),
    ILLEGAL_OVERFLOW_MODE(157, "ILLEGAL_OVERFLOW_MODE"),
    TOO_MANY_ROWS(158, "TOO_MANY_ROWS"),
    TIMEOUT_EXCEEDED(159, "TIMEOUT_EXCEEDED"),
    TOO_SLOW(160, "TOO_SLOW"),
    TOO_MANY_COLUMNS(161, "TOO_MANY_COLUMNS"),
    TOO_DEEP_SUBQUERIES(162, "TOO_DEEP_SUBQUERIES"),
    TOO_DEEP_PIPELINE(163, "TOO_DEEP_PIPELINE"),
    READONLY(164, "READONLY"),
    TOO_MANY_TEMPORARY_COLUMNS(165, "TOO_MANY_TEMPORARY_COLUMNS"),
    TOO_MANY_TEMPORARY_NON_CONST_COLUMNS(166, "TOO_MANY_TEMPORARY_NON_CONST_COLUMNS"),
    TOO_DEEP_AST(167, "TOO_DEEP_AST"),
    TOO_BIG_AST(168, "TOO_BIG_AST"),
    BAD_TYPE_OF_FIELD(169, "BAD_TYPE_OF_FIELD"),
    BAD_GET(170, "BAD_GET"),
    CANNOT_CREATE_DIRECTORY(172, "CANNOT_CREATE_DIRECTORY"),
    CANNOT_ALLOCATE_MEMORY(173, "CANNOT_ALLOCATE_MEMORY"),
    CYCLIC_ALIASES(174, "CYCLIC_ALIASES"),
    CHUNK_NOT_FOUND(176, "CHUNK_NOT_FOUND"),
    DUPLICATE_CHUNK_NAME(177, "DUPLICATE_CHUNK_NAME"),
    MULTIPLE_ALIASES_FOR_EXPRESSION(178, "MULTIPLE_ALIASES_FOR_EXPRESSION"),
    MULTIPLE_EXPRESSIONS_FOR_ALIAS(179, "MULTIPLE_EXPRESSIONS_FOR_ALIAS"),
    THERE_IS_NO_PROFILE(180, "THERE_IS_NO_PROFILE"),
    ILLEGAL_FINAL(181, "ILLEGAL_FINAL"),
    ILLEGAL_PREWHERE(182, "ILLEGAL_PREWHERE"),
    UNEXPECTED_EXPRESSION(183, "UNEXPECTED_EXPRESSION"),
    ILLEGAL_AGGREGATION(184, "ILLEGAL_AGGREGATION"),
    UNSUPPORTED_MYISAM_BLOCK_TYPE(185, "UNSUPPORTED_MYISAM_BLOCK_TYPE"),
    UNSUPPORTED_COLLATION_LOCALE(186, "UNSUPPORTED_COLLATION_LOCALE"),
    COLLATION_COMPARISON_FAILED(187, "COLLATION_COMPARISON_FAILED"),
    UNKNOWN_ACTION(188, "UNKNOWN_ACTION"),
    TABLE_MUST_NOT_BE_CREATED_MANUALLY(189, "TABLE_MUST_NOT_BE_CREATED_MANUALLY"),
    SIZES_OF_ARRAYS_DOESNT_MATCH(190, "SIZES_OF_ARRAYS_DOESNT_MATCH"),
    SET_SIZE_LIMIT_EXCEEDED(191, "SET_SIZE_LIMIT_EXCEEDED"),
    UNKNOWN_USER(192, "UNKNOWN_USER"),
    WRONG_PASSWORD(193, "WRONG_PASSWORD"),
    REQUIRED_PASSWORD(194, "REQUIRED_PASSWORD"),
    IP_ADDRESS_NOT_ALLOWED(195, "IP_ADDRESS_NOT_ALLOWED"),
    UNKNOWN_ADDRESS_PATTERN_TYPE(196, "UNKNOWN_ADDRESS_PATTERN_TYPE"),
    SERVER_REVISION_IS_TOO_OLD(197, "SERVER_REVISION_IS_TOO_OLD"),
    DNS_ERROR(198, "DNS_ERROR"),
    UNKNOWN_QUOTA(199, "UNKNOWN_QUOTA"),
    QUOTA_DOESNT_ALLOW_KEYS(200, "QUOTA_DOESNT_ALLOW_KEYS"),
    QUOTA_EXPIRED(201, "QUOTA_EXPIRED"),
    TOO_MANY_SIMULTANEOUS_QUERIES(202, "TOO_MANY_SIMULTANEOUS_QUERIES"),
    NO_FREE_CONNECTION(203, "NO_FREE_CONNECTION"),
    CANNOT_FSYNC(204, "CANNOT_FSYNC"),
    NESTED_TYPE_TOO_DEEP(205, "NESTED_TYPE_TOO_DEEP"),
    ALIAS_REQUIRED(206, "ALIAS_REQUIRED"),
    AMBIGUOUS_IDENTIFIER(207, "AMBIGUOUS_IDENTIFIER"),
    EMPTY_NESTED_TABLE(208, "EMPTY_NESTED_TABLE"),
    SOCKET_TIMEOUT(209, "SOCKET_TIMEOUT"),
    NETWORK_ERROR(210, "NETWORK_ERROR"),
    EMPTY_QUERY(211, "EMPTY_QUERY"),
    UNKNOWN_LOAD_BALANCING(212, "UNKNOWN_LOAD_BALANCING"),
    UNKNOWN_TOTALS_MODE(213, "UNKNOWN_TOTALS_MODE"),
    CANNOT_STATVFS(214, "CANNOT_STATVFS"),
    NOT_AN_AGGREGATE(215, "NOT_AN_AGGREGATE"),
    QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING(216, "QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING"),
    CLIENT_HAS_CONNECTED_TO_WRONG_PORT(217, "CLIENT_HAS_CONNECTED_TO_WRONG_PORT"),
    TABLE_IS_DROPPED(218, "TABLE_IS_DROPPED"),
    DATABASE_NOT_EMPTY(219, "DATABASE_NOT_EMPTY"),
    DUPLICATE_INTERSERVER_IO_ENDPOINT(220, "DUPLICATE_INTERSERVER_IO_ENDPOINT"),
    NO_SUCH_INTERSERVER_IO_ENDPOINT(221, "NO_SUCH_INTERSERVER_IO_ENDPOINT"),
    ADDING_REPLICA_TO_NON_EMPTY_TABLE(222, "ADDING_REPLICA_TO_NON_EMPTY_TABLE"),
    UNEXPECTED_AST_STRUCTURE(223, "UNEXPECTED_AST_STRUCTURE"),
    REPLICA_IS_ALREADY_ACTIVE(224, "REPLICA_IS_ALREADY_ACTIVE"),
    NO_ZOOKEEPER(225, "NO_ZOOKEEPER"),
    NO_FILE_IN_DATA_PART(226, "NO_FILE_IN_DATA_PART"),
    UNEXPECTED_FILE_IN_DATA_PART(227, "UNEXPECTED_FILE_IN_DATA_PART"),
    BAD_SIZE_OF_FILE_IN_DATA_PART(228, "BAD_SIZE_OF_FILE_IN_DATA_PART"),
    QUERY_IS_TOO_LARGE(229, "QUERY_IS_TOO_LARGE"),
    NOT_FOUND_EXPECTED_DATA_PART(230, "NOT_FOUND_EXPECTED_DATA_PART"),
    TOO_MANY_UNEXPECTED_DATA_PARTS(231, "TOO_MANY_UNEXPECTED_DATA_PARTS"),
    NO_SUCH_DATA_PART(232, "NO_SUCH_DATA_PART"),
    BAD_DATA_PART_NAME(233, "BAD_DATA_PART_NAME"),
    NO_REPLICA_HAS_PART(234, "NO_REPLICA_HAS_PART"),
    DUPLICATE_DATA_PART(235, "DUPLICATE_DATA_PART"),
    ABORTED(236, "ABORTED"),
    NO_REPLICA_NAME_GIVEN(237, "NO_REPLICA_NAME_GIVEN"),
    FORMAT_VERSION_TOO_OLD(238, "FORMAT_VERSION_TOO_OLD"),
    CANNOT_MUNMAP(239, "CANNOT_MUNMAP"),
    CANNOT_MREMAP(240, "CANNOT_MREMAP"),
    MEMORY_LIMIT_EXCEEDED(241, "MEMORY_LIMIT_EXCEEDED"),
    TABLE_IS_READ_ONLY(242, "TABLE_IS_READ_ONLY"),
    NOT_ENOUGH_SPACE(243, "NOT_ENOUGH_SPACE"),
    UNEXPECTED_ZOOKEEPER_ERROR(244, "UNEXPECTED_ZOOKEEPER_ERROR"),
    CORRUPTED_DATA(246, "CORRUPTED_DATA"),
    INCORRECT_MARK(247, "INCORRECT_MARK"),
    INVALID_PARTITION_VALUE(248, "INVALID_PARTITION_VALUE"),
    NOT_ENOUGH_BLOCK_NUMBERS(250, "NOT_ENOUGH_BLOCK_NUMBERS"),
    NO_SUCH_REPLICA(251, "NO_SUCH_REPLICA"),
    TOO_MANY_PARTS(252, "TOO_MANY_PARTS"),
    REPLICA_IS_ALREADY_EXIST(253, "REPLICA_IS_ALREADY_EXIST"),
    NO_ACTIVE_REPLICAS(254, "NO_ACTIVE_REPLICAS"),
    TOO_MANY_RETRIES_TO_FETCH_PARTS(255, "TOO_MANY_RETRIES_TO_FETCH_PARTS"),
    PARTITION_ALREADY_EXISTS(256, "PARTITION_ALREADY_EXISTS"),
    PARTITION_DOESNT_EXIST(257, "PARTITION_DOESNT_EXIST"),
    UNION_ALL_RESULT_STRUCTURES_MISMATCH(258, "UNION_ALL_RESULT_STRUCTURES_MISMATCH"),
    CLIENT_OUTPUT_FORMAT_SPECIFIED(260, "CLIENT_OUTPUT_FORMAT_SPECIFIED"),
    UNKNOWN_BLOCK_INFO_FIELD(261, "UNKNOWN_BLOCK_INFO_FIELD"),
    BAD_COLLATION(262, "BAD_COLLATION"),
    CANNOT_COMPILE_CODE(263, "CANNOT_COMPILE_CODE"),
    INCOMPATIBLE_TYPE_OF_JOIN(264, "INCOMPATIBLE_TYPE_OF_JOIN"),
    NO_AVAILABLE_REPLICA(265, "NO_AVAILABLE_REPLICA"),
    MISMATCH_REPLICAS_DATA_SOURCES(266, "MISMATCH_REPLICAS_DATA_SOURCES"),
    STORAGE_DOESNT_SUPPORT_PARALLEL_REPLICAS(267, "STORAGE_DOESNT_SUPPORT_PARALLEL_REPLICAS"),
    CPUID_ERROR(268, "CPUID_ERROR"),
    INFINITE_LOOP(269, "INFINITE_LOOP"),
    CANNOT_COMPRESS(270, "CANNOT_COMPRESS"),
    CANNOT_DECOMPRESS(271, "CANNOT_DECOMPRESS"),
    CANNOT_IO_SUBMIT(272, "CANNOT_IO_SUBMIT"),
    CANNOT_IO_GETEVENTS(273, "CANNOT_IO_GETEVENTS"),
    AIO_READ_ERROR(274, "AIO_READ_ERROR"),
    AIO_WRITE_ERROR(275, "AIO_WRITE_ERROR"),
    INDEX_NOT_USED(277, "INDEX_NOT_USED"),
    ALL_CONNECTION_TRIES_FAILED(279, "ALL_CONNECTION_TRIES_FAILED"),
    NO_AVAILABLE_DATA(280, "NO_AVAILABLE_DATA"),
    DICTIONARY_IS_EMPTY(281, "DICTIONARY_IS_EMPTY"),
    INCORRECT_INDEX(282, "INCORRECT_INDEX"),
    UNKNOWN_DISTRIBUTED_PRODUCT_MODE(283, "UNKNOWN_DISTRIBUTED_PRODUCT_MODE"),
    WRONG_GLOBAL_SUBQUERY(284, "WRONG_GLOBAL_SUBQUERY"),
    TOO_FEW_LIVE_REPLICAS(285, "TOO_FEW_LIVE_REPLICAS"),
    UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE(286, "UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE"),
    UNKNOWN_FORMAT_VERSION(287, "UNKNOWN_FORMAT_VERSION"),
    DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED(288, "DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED"),
    REPLICA_IS_NOT_IN_QUORUM(289, "REPLICA_IS_NOT_IN_QUORUM"),
    LIMIT_EXCEEDED(290, "LIMIT_EXCEEDED"),
    DATABASE_ACCESS_DENIED(291, "DATABASE_ACCESS_DENIED"),
    MONGODB_CANNOT_AUTHENTICATE(293, "MONGODB_CANNOT_AUTHENTICATE"),
    INVALID_BLOCK_EXTRA_INFO(294, "INVALID_BLOCK_EXTRA_INFO"),
    RECEIVED_EMPTY_DATA(295, "RECEIVED_EMPTY_DATA"),
    NO_REMOTE_SHARD_FOUND(296, "NO_REMOTE_SHARD_FOUND"),
    SHARD_HAS_NO_CONNECTIONS(297, "SHARD_HAS_NO_CONNECTIONS"),
    CANNOT_PIPE(298, "CANNOT_PIPE"),
    CANNOT_FORK(299, "CANNOT_FORK"),
    CANNOT_DLSYM(300, "CANNOT_DLSYM"),
    CANNOT_CREATE_CHILD_PROCESS(301, "CANNOT_CREATE_CHILD_PROCESS"),
    CHILD_WAS_NOT_EXITED_NORMALLY(302, "CHILD_WAS_NOT_EXITED_NORMALLY"),
    CANNOT_SELECT(303, "CANNOT_SELECT"),
    CANNOT_WAITPID(304, "CANNOT_WAITPID"),
    TABLE_WAS_NOT_DROPPED(305, "TABLE_WAS_NOT_DROPPED"),
    TOO_DEEP_RECURSION(306, "TOO_DEEP_RECURSION"),
    TOO_MANY_BYTES(307, "TOO_MANY_BYTES"),
    UNEXPECTED_NODE_IN_ZOOKEEPER(308, "UNEXPECTED_NODE_IN_ZOOKEEPER"),
    FUNCTION_CANNOT_HAVE_PARAMETERS(309, "FUNCTION_CANNOT_HAVE_PARAMETERS"),
    INVALID_SHARD_WEIGHT(317, "INVALID_SHARD_WEIGHT"),
    INVALID_CONFIG_PARAMETER(318, "INVALID_CONFIG_PARAMETER"),
    UNKNOWN_STATUS_OF_INSERT(319, "UNKNOWN_STATUS_OF_INSERT"),
    VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE(321, "VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE"),
    BARRIER_TIMEOUT(335, "BARRIER_TIMEOUT"),
    UNKNOWN_DATABASE_ENGINE(336, "UNKNOWN_DATABASE_ENGINE"),
    DDL_GUARD_IS_ACTIVE(337, "DDL_GUARD_IS_ACTIVE"),
    UNFINISHED(341, "UNFINISHED"),
    METADATA_MISMATCH(342, "METADATA_MISMATCH"),
    SUPPORT_IS_DISABLED(344, "SUPPORT_IS_DISABLED"),
    TABLE_DIFFERS_TOO_MUCH(345, "TABLE_DIFFERS_TOO_MUCH"),
    CANNOT_CONVERT_CHARSET(346, "CANNOT_CONVERT_CHARSET"),
    CANNOT_LOAD_CONFIG(347, "CANNOT_LOAD_CONFIG"),
    CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN(349, "CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN"),
    INCOMPATIBLE_SOURCE_TABLES(350, "INCOMPATIBLE_SOURCE_TABLES"),
    AMBIGUOUS_TABLE_NAME(351, "AMBIGUOUS_TABLE_NAME"),
    AMBIGUOUS_COLUMN_NAME(352, "AMBIGUOUS_COLUMN_NAME"),
    INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE(353, "INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE"),
    ZLIB_INFLATE_FAILED(354, "ZLIB_INFLATE_FAILED"),
    ZLIB_DEFLATE_FAILED(355, "ZLIB_DEFLATE_FAILED"),
    BAD_LAMBDA(356, "BAD_LAMBDA"),
    RESERVED_IDENTIFIER_NAME(357, "RESERVED_IDENTIFIER_NAME"),
    INTO_OUTFILE_NOT_ALLOWED(358, "INTO_OUTFILE_NOT_ALLOWED"),
    TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT(359, "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT"),
    CANNOT_CREATE_CHARSET_CONVERTER(360, "CANNOT_CREATE_CHARSET_CONVERTER"),
    SEEK_POSITION_OUT_OF_BOUND(361, "SEEK_POSITION_OUT_OF_BOUND"),
    CURRENT_WRITE_BUFFER_IS_EXHAUSTED(362, "CURRENT_WRITE_BUFFER_IS_EXHAUSTED"),
    CANNOT_CREATE_IO_BUFFER(363, "CANNOT_CREATE_IO_BUFFER"),
    RECEIVED_ERROR_TOO_MANY_REQUESTS(364, "RECEIVED_ERROR_TOO_MANY_REQUESTS"),
    SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT(366, "SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT"),
    TOO_MANY_FETCHES(367, "TOO_MANY_FETCHES"),
    ALL_REPLICAS_ARE_STALE(369, "ALL_REPLICAS_ARE_STALE"),
    DATA_TYPE_CANNOT_BE_USED_IN_TABLES(370, "DATA_TYPE_CANNOT_BE_USED_IN_TABLES"),
    INCONSISTENT_CLUSTER_DEFINITION(371, "INCONSISTENT_CLUSTER_DEFINITION"),
    SESSION_NOT_FOUND(372, "SESSION_NOT_FOUND"),
    SESSION_IS_LOCKED(373, "SESSION_IS_LOCKED"),
    INVALID_SESSION_TIMEOUT(374, "INVALID_SESSION_TIMEOUT"),
    CANNOT_DLOPEN(375, "CANNOT_DLOPEN"),
    CANNOT_PARSE_UUID(376, "CANNOT_PARSE_UUID"),
    ILLEGAL_SYNTAX_FOR_DATA_TYPE(377, "ILLEGAL_SYNTAX_FOR_DATA_TYPE"),
    DATA_TYPE_CANNOT_HAVE_ARGUMENTS(378, "DATA_TYPE_CANNOT_HAVE_ARGUMENTS"),
    UNKNOWN_STATUS_OF_DISTRIBUTED_DDL_TASK(379, "UNKNOWN_STATUS_OF_DISTRIBUTED_DDL_TASK"),
    CANNOT_KILL(380, "CANNOT_KILL"),
    HTTP_LENGTH_REQUIRED(381, "HTTP_LENGTH_REQUIRED"),
    CANNOT_LOAD_CATBOOST_MODEL(382, "CANNOT_LOAD_CATBOOST_MODEL"),
    CANNOT_APPLY_CATBOOST_MODEL(383, "CANNOT_APPLY_CATBOOST_MODEL"),
    PART_IS_TEMPORARILY_LOCKED(384, "PART_IS_TEMPORARILY_LOCKED"),
    MULTIPLE_STREAMS_REQUIRED(385, "MULTIPLE_STREAMS_REQUIRED"),
    NO_COMMON_TYPE(386, "NO_COMMON_TYPE"),
    DICTIONARY_ALREADY_EXISTS(387, "DICTIONARY_ALREADY_EXISTS"),
    CANNOT_ASSIGN_OPTIMIZE(388, "CANNOT_ASSIGN_OPTIMIZE"),
    INSERT_WAS_DEDUPLICATED(389, "INSERT_WAS_DEDUPLICATED"),
    CANNOT_GET_CREATE_TABLE_QUERY(390, "CANNOT_GET_CREATE_TABLE_QUERY"),
    EXTERNAL_LIBRARY_ERROR(391, "EXTERNAL_LIBRARY_ERROR"),
    QUERY_IS_PROHIBITED(392, "QUERY_IS_PROHIBITED"),
    THERE_IS_NO_QUERY(393, "THERE_IS_NO_QUERY"),
    QUERY_WAS_CANCELLED(394, "QUERY_WAS_CANCELLED"),
    FUNCTION_THROW_IF_VALUE_IS_NON_ZERO(395, "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"),
    TOO_MANY_ROWS_OR_BYTES(396, "TOO_MANY_ROWS_OR_BYTES"),
    QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW(397, "QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW"),
    UNKNOWN_MUTATION_COMMAND(398, "UNKNOWN_MUTATION_COMMAND"),
    FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT(399, "FORMAT_IS_NOT_SUITABLE_FOR_OUTPUT"),
    CANNOT_STAT(400, "CANNOT_STAT"),
    FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME(401, "FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME"),
    CANNOT_IOSETUP(402, "CANNOT_IOSETUP"),
    INVALID_JOIN_ON_EXPRESSION(403, "INVALID_JOIN_ON_EXPRESSION"),
    BAD_ODBC_CONNECTION_STRING(404, "BAD_ODBC_CONNECTION_STRING"),
    PARTITION_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT(405, "PARTITION_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT"),
    TOP_AND_LIMIT_TOGETHER(406, "TOP_AND_LIMIT_TOGETHER"),
    DECIMAL_OVERFLOW(407, "DECIMAL_OVERFLOW"),
    BAD_REQUEST_PARAMETER(408, "BAD_REQUEST_PARAMETER"),
    EXTERNAL_EXECUTABLE_NOT_FOUND(409, "EXTERNAL_EXECUTABLE_NOT_FOUND"),
    EXTERNAL_SERVER_IS_NOT_RESPONDING(410, "EXTERNAL_SERVER_IS_NOT_RESPONDING"),
    PTHREAD_ERROR(411, "PTHREAD_ERROR"),
    NETLINK_ERROR(412, "NETLINK_ERROR"),
    CANNOT_SET_SIGNAL_HANDLER(413, "CANNOT_SET_SIGNAL_HANDLER"),
    ALL_REPLICAS_LOST(415, "ALL_REPLICAS_LOST"),
    REPLICA_STATUS_CHANGED(416, "REPLICA_STATUS_CHANGED"),
    EXPECTED_ALL_OR_ANY(417, "EXPECTED_ALL_OR_ANY"),
    UNKNOWN_JOIN(418, "UNKNOWN_JOIN"),
    MULTIPLE_ASSIGNMENTS_TO_COLUMN(419, "MULTIPLE_ASSIGNMENTS_TO_COLUMN"),
    CANNOT_UPDATE_COLUMN(420, "CANNOT_UPDATE_COLUMN"),
    CANNOT_ADD_DIFFERENT_AGGREGATE_STATES(421, "CANNOT_ADD_DIFFERENT_AGGREGATE_STATES"),
    UNSUPPORTED_URI_SCHEME(422, "UNSUPPORTED_URI_SCHEME"),
    CANNOT_GETTIMEOFDAY(423, "CANNOT_GETTIMEOFDAY"),
    CANNOT_LINK(424, "CANNOT_LINK"),
    SYSTEM_ERROR(425, "SYSTEM_ERROR"),
    CANNOT_COMPILE_REGEXP(427, "CANNOT_COMPILE_REGEXP"),
    UNKNOWN_LOG_LEVEL(428, "UNKNOWN_LOG_LEVEL"),
    FAILED_TO_GETPWUID(429, "FAILED_TO_GETPWUID"),
    MISMATCHING_USERS_FOR_PROCESS_AND_DATA(430, "MISMATCHING_USERS_FOR_PROCESS_AND_DATA"),
    ILLEGAL_SYNTAX_FOR_CODEC_TYPE(431, "ILLEGAL_SYNTAX_FOR_CODEC_TYPE"),
    UNKNOWN_CODEC(432, "UNKNOWN_CODEC"),
    ILLEGAL_CODEC_PARAMETER(433, "ILLEGAL_CODEC_PARAMETER"),
    CANNOT_PARSE_PROTOBUF_SCHEMA(434, "CANNOT_PARSE_PROTOBUF_SCHEMA"),
    NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD(435, "NO_DATA_FOR_REQUIRED_PROTOBUF_FIELD"),
    PROTOBUF_BAD_CAST(436, "PROTOBUF_BAD_CAST"),
    PROTOBUF_FIELD_NOT_REPEATED(437, "PROTOBUF_FIELD_NOT_REPEATED"),
    DATA_TYPE_CANNOT_BE_PROMOTED(438, "DATA_TYPE_CANNOT_BE_PROMOTED"),
    CANNOT_SCHEDULE_TASK(439, "CANNOT_SCHEDULE_TASK"),
    INVALID_LIMIT_EXPRESSION(440, "INVALID_LIMIT_EXPRESSION"),
    CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING(441, "CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING"),
    BAD_DATABASE_FOR_TEMPORARY_TABLE(442, "BAD_DATABASE_FOR_TEMPORARY_TABLE"),
    NO_COMMON_COLUMNS_WITH_PROTOBUF_SCHEMA(443, "NO_COMMON_COLUMNS_WITH_PROTOBUF_SCHEMA"),
    UNKNOWN_PROTOBUF_FORMAT(444, "UNKNOWN_PROTOBUF_FORMAT"),
    CANNOT_MPROTECT(445, "CANNOT_MPROTECT"),
    FUNCTION_NOT_ALLOWED(446, "FUNCTION_NOT_ALLOWED"),
    HYPERSCAN_CANNOT_SCAN_TEXT(447, "HYPERSCAN_CANNOT_SCAN_TEXT"),
    BROTLI_READ_FAILED(448, "BROTLI_READ_FAILED"),
    BROTLI_WRITE_FAILED(449, "BROTLI_WRITE_FAILED"),
    BAD_TTL_EXPRESSION(450, "BAD_TTL_EXPRESSION"),
    BAD_TTL_FILE(451, "BAD_TTL_FILE"),
    SETTING_CONSTRAINT_VIOLATION(452, "SETTING_CONSTRAINT_VIOLATION"),
    MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES(453, "MYSQL_CLIENT_INSUFFICIENT_CAPABILITIES"),
    OPENSSL_ERROR(454, "OPENSSL_ERROR"),
    SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY(455, "SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY"),
    UNKNOWN_QUERY_PARAMETER(456, "UNKNOWN_QUERY_PARAMETER"),
    BAD_QUERY_PARAMETER(457, "BAD_QUERY_PARAMETER"),
    CANNOT_UNLINK(458, "CANNOT_UNLINK"),
    CANNOT_SET_THREAD_PRIORITY(459, "CANNOT_SET_THREAD_PRIORITY"),
    CANNOT_CREATE_TIMER(460, "CANNOT_CREATE_TIMER"),
    CANNOT_SET_TIMER_PERIOD(461, "CANNOT_SET_TIMER_PERIOD"),
    CANNOT_DELETE_TIMER(462, "CANNOT_DELETE_TIMER"),
    CANNOT_FCNTL(463, "CANNOT_FCNTL"),
    CANNOT_PARSE_ELF(464, "CANNOT_PARSE_ELF"),
    CANNOT_PARSE_DWARF(465, "CANNOT_PARSE_DWARF"),
    INSECURE_PATH(466, "INSECURE_PATH"),
    CANNOT_PARSE_BOOL(467, "CANNOT_PARSE_BOOL"),
    CANNOT_PTHREAD_ATTR(468, "CANNOT_PTHREAD_ATTR"),
    VIOLATED_CONSTRAINT(469, "VIOLATED_CONSTRAINT"),
    QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW(470, "QUERY_IS_NOT_SUPPORTED_IN_LIVE_VIEW"),
    INVALID_SETTING_VALUE(471, "INVALID_SETTING_VALUE"),
    READONLY_SETTING(472, "READONLY_SETTING"),
    DEADLOCK_AVOIDED(473, "DEADLOCK_AVOIDED"),
    INVALID_TEMPLATE_FORMAT(474, "INVALID_TEMPLATE_FORMAT"),
    INVALID_WITH_FILL_EXPRESSION(475, "INVALID_WITH_FILL_EXPRESSION"),
    WITH_TIES_WITHOUT_ORDER_BY(476, "WITH_TIES_WITHOUT_ORDER_BY"),
    INVALID_USAGE_OF_INPUT(477, "INVALID_USAGE_OF_INPUT"),
    UNKNOWN_POLICY(478, "UNKNOWN_POLICY"),
    UNKNOWN_DISK(479, "UNKNOWN_DISK"),
    UNKNOWN_PROTOCOL(480, "UNKNOWN_PROTOCOL"),
    PATH_ACCESS_DENIED(481, "PATH_ACCESS_DENIED"),
    DICTIONARY_ACCESS_DENIED(482, "DICTIONARY_ACCESS_DENIED"),
    TOO_MANY_REDIRECTS(483, "TOO_MANY_REDIRECTS"),
    INTERNAL_REDIS_ERROR(484, "INTERNAL_REDIS_ERROR"),
    SCALAR_ALREADY_EXISTS(485, "SCALAR_ALREADY_EXISTS"),
    CANNOT_GET_CREATE_DICTIONARY_QUERY(487, "CANNOT_GET_CREATE_DICTIONARY_QUERY"),
    UNKNOWN_DICTIONARY(488, "UNKNOWN_DICTIONARY"),
    INCORRECT_DICTIONARY_DEFINITION(489, "INCORRECT_DICTIONARY_DEFINITION"),
    CANNOT_FORMAT_DATETIME(490, "CANNOT_FORMAT_DATETIME"),
    UNACCEPTABLE_URL(491, "UNACCEPTABLE_URL"),
    ACCESS_ENTITY_NOT_FOUND(492, "ACCESS_ENTITY_NOT_FOUND"),
    ACCESS_ENTITY_ALREADY_EXISTS(493, "ACCESS_ENTITY_ALREADY_EXISTS"),
    ACCESS_ENTITY_FOUND_DUPLICATES(494, "ACCESS_ENTITY_FOUND_DUPLICATES"),
    ACCESS_STORAGE_READONLY(495, "ACCESS_STORAGE_READONLY"),
    QUOTA_REQUIRES_CLIENT_KEY(496, "QUOTA_REQUIRES_CLIENT_KEY"),
    ACCESS_DENIED(497, "ACCESS_DENIED"),
    LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED(498, "LIMIT_BY_WITH_TIES_IS_NOT_SUPPORTED"),
    S3_ERROR(499, "S3_ERROR"),
    CANNOT_CREATE_DATABASE(501, "CANNOT_CREATE_DATABASE"),
    CANNOT_SIGQUEUE(502, "CANNOT_SIGQUEUE"),
    AGGREGATE_FUNCTION_THROW(503, "AGGREGATE_FUNCTION_THROW"),
    FILE_ALREADY_EXISTS(504, "FILE_ALREADY_EXISTS"),
    CANNOT_DELETE_DIRECTORY(505, "CANNOT_DELETE_DIRECTORY"),
    UNEXPECTED_ERROR_CODE(506, "UNEXPECTED_ERROR_CODE"),
    UNABLE_TO_SKIP_UNUSED_SHARDS(507, "UNABLE_TO_SKIP_UNUSED_SHARDS"),
    UNKNOWN_ACCESS_TYPE(508, "UNKNOWN_ACCESS_TYPE"),
    INVALID_GRANT(509, "INVALID_GRANT"),
    CACHE_DICTIONARY_UPDATE_FAIL(510, "CACHE_DICTIONARY_UPDATE_FAIL"),
    UNKNOWN_ROLE(511, "UNKNOWN_ROLE"),
    SET_NON_GRANTED_ROLE(512, "SET_NON_GRANTED_ROLE"),
    UNKNOWN_PART_TYPE(513, "UNKNOWN_PART_TYPE"),
    ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND(514, "ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND"),
    INCORRECT_ACCESS_ENTITY_DEFINITION(515, "INCORRECT_ACCESS_ENTITY_DEFINITION"),
    AUTHENTICATION_FAILED(516, "AUTHENTICATION_FAILED"),
    CANNOT_ASSIGN_ALTER(517, "CANNOT_ASSIGN_ALTER"),
    CANNOT_COMMIT_OFFSET(518, "CANNOT_COMMIT_OFFSET"),
    NO_REMOTE_SHARD_AVAILABLE(519, "NO_REMOTE_SHARD_AVAILABLE"),
    CANNOT_DETACH_DICTIONARY_AS_TABLE(520, "CANNOT_DETACH_DICTIONARY_AS_TABLE"),
    ATOMIC_RENAME_FAIL(521, "ATOMIC_RENAME_FAIL"),
    UNKNOWN_ROW_POLICY(523, "UNKNOWN_ROW_POLICY"),
    ALTER_OF_COLUMN_IS_FORBIDDEN(524, "ALTER_OF_COLUMN_IS_FORBIDDEN"),
    INCORRECT_DISK_INDEX(525, "INCORRECT_DISK_INDEX"),
    UNKNOWN_VOLUME_TYPE(526, "UNKNOWN_VOLUME_TYPE"),
    NO_SUITABLE_FUNCTION_IMPLEMENTATION(527, "NO_SUITABLE_FUNCTION_IMPLEMENTATION"),
    CASSANDRA_INTERNAL_ERROR(528, "CASSANDRA_INTERNAL_ERROR"),
    NOT_A_LEADER(529, "NOT_A_LEADER"),
    CANNOT_CONNECT_RABBITMQ(530, "CANNOT_CONNECT_RABBITMQ"),
    CANNOT_FSTAT(531, "CANNOT_FSTAT"),
    LDAP_ERROR(532, "LDAP_ERROR"),
    INCONSISTENT_RESERVATIONS(533, "INCONSISTENT_RESERVATIONS"),
    NO_RESERVATIONS_PROVIDED(534, "NO_RESERVATIONS_PROVIDED"),
    UNKNOWN_RAID_TYPE(535, "UNKNOWN_RAID_TYPE"),
    CANNOT_RESTORE_FROM_FIELD_DUMP(536, "CANNOT_RESTORE_FROM_FIELD_DUMP"),
    ILLEGAL_MYSQL_VARIABLE(537, "ILLEGAL_MYSQL_VARIABLE"),
    MYSQL_SYNTAX_ERROR(538, "MYSQL_SYNTAX_ERROR"),
    CANNOT_BIND_RABBITMQ_EXCHANGE(539, "CANNOT_BIND_RABBITMQ_EXCHANGE"),
    CANNOT_DECLARE_RABBITMQ_EXCHANGE(540, "CANNOT_DECLARE_RABBITMQ_EXCHANGE"),
    CANNOT_CREATE_RABBITMQ_QUEUE_BINDING(541, "CANNOT_CREATE_RABBITMQ_QUEUE_BINDING"),
    CANNOT_REMOVE_RABBITMQ_EXCHANGE(542, "CANNOT_REMOVE_RABBITMQ_EXCHANGE"),
    UNKNOWN_MYSQL_DATATYPES_SUPPORT_LEVEL(543, "UNKNOWN_MYSQL_DATATYPES_SUPPORT_LEVEL"),
    ROW_AND_ROWS_TOGETHER(544, "ROW_AND_ROWS_TOGETHER"),
    FIRST_AND_NEXT_TOGETHER(545, "FIRST_AND_NEXT_TOGETHER"),
    NO_ROW_DELIMITER(546, "NO_ROW_DELIMITER"),
    INVALID_RAID_TYPE(547, "INVALID_RAID_TYPE"),
    UNKNOWN_VOLUME(548, "UNKNOWN_VOLUME"),
    DATA_TYPE_CANNOT_BE_USED_IN_KEY(549, "DATA_TYPE_CANNOT_BE_USED_IN_KEY"),
    CONDITIONAL_TREE_PARENT_NOT_FOUND(550, "CONDITIONAL_TREE_PARENT_NOT_FOUND"),
    ILLEGAL_PROJECTION_MANIPULATOR(551, "ILLEGAL_PROJECTION_MANIPULATOR"),
    UNRECOGNIZED_ARGUMENTS(552, "UNRECOGNIZED_ARGUMENTS"),
    LZMA_STREAM_ENCODER_FAILED(553, "LZMA_STREAM_ENCODER_FAILED"),
    LZMA_STREAM_DECODER_FAILED(554, "LZMA_STREAM_DECODER_FAILED"),
    ROCKSDB_ERROR(555, "ROCKSDB_ERROR"),
    SYNC_MYSQL_USER_ACCESS_ERROR(556, "SYNC_MYSQL_USER_ACCESS_ERROR"),
    UNKNOWN_UNION(557, "UNKNOWN_UNION"),
    EXPECTED_ALL_OR_DISTINCT(558, "EXPECTED_ALL_OR_DISTINCT"),
    INVALID_GRPC_QUERY_INFO(559, "INVALID_GRPC_QUERY_INFO"),
    ZSTD_ENCODER_FAILED(560, "ZSTD_ENCODER_FAILED"),
    ZSTD_DECODER_FAILED(561, "ZSTD_DECODER_FAILED"),
    TLD_LIST_NOT_FOUND(562, "TLD_LIST_NOT_FOUND"),
    CANNOT_READ_MAP_FROM_TEXT(563, "CANNOT_READ_MAP_FROM_TEXT"),
    INTERSERVER_SCHEME_DOESNT_MATCH(564, "INTERSERVER_SCHEME_DOESNT_MATCH"),
    TOO_MANY_PARTITIONS(565, "TOO_MANY_PARTITIONS"),
    CANNOT_RMDIR(566, "CANNOT_RMDIR"),
    DUPLICATED_PART_UUIDS(567, "DUPLICATED_PART_UUIDS"),
    KEEPER_EXCEPTION(999, "KEEPER_EXCEPTION"),
    POCO_EXCEPTION(1000, "POCO_EXCEPTION"),
    STD_EXCEPTION(1001, "STD_EXCEPTION"),
    UNKNOWN_EXCEPTION(1002, "UNKNOWN_EXCEPTION"),
    INVALID_SHARD_ID(1003, "INVALID_SHARD_ID");

    private final int code;

    private final String name;

    ByteHouseErrCode(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static ByteHouseErrCode fromCode(int code) {
        for (ByteHouseErrCode value : ByteHouseErrCode.values()) {
            if (value.code == code)
                return value;
        }
        return ByteHouseErrCode.UNKNOWN_ERROR;
    }

    public int code() {
        return code;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "[%s] %s", code, name);
    }
}
