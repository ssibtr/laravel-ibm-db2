<?php

namespace SystemServices\DB2\Database\Connectors;

/**
 * Class ODBCConnector
 *
 * @package SystemServices\DB2\Database\Connectors
 */
class ODBCConnector extends DB2Connector
{
    /**
     * @param array $config
     *
     * @return string
     */
    protected function getDsn(array $config): string
    {
        $dsnParts = [
            'odbc:DRIVER=%s',
            'System=%s',
            'Database=%s',
            'UserID=%s',
            'Password=%s',
        ];

        $dsnConfig = [
            $config['driverName'],
            $config['host'],
            $config['database'],
            $config['username'],
            $config['password'],
        ];

        if (array_key_exists('odbc_keywords', $config)) {
            $odbcKeywords = $config['odbc_keywords'];
            $parts = array_map(static function ($part) {
                return $part . '=%s';
            }, array_keys($odbcKeywords));
            $config = array_values($odbcKeywords);

            $dsnParts = array_merge($dsnParts, $parts);
            $dsnConfig = array_merge($dsnConfig, $config);
        }

        return sprintf(implode(';', $dsnParts), ...$dsnConfig);
    }
}
