<?php

namespace GuidoFaecke\DB2\Database\Connectors;

/**
 * Class IBMConnector
 *
 * @package GuidoFaecke\DB2\Database\Connectors
 */
class IBMConnector extends DB2Connector
{
    /**
     * @param array $config
     *
     * @return string
     */
    protected function getDsn(array $config): string
    {
        return sprintf(
            "ibm:DRIVER={%s};DATABASE={%s};HOSTNAME={%s};PORT={%s};PROTOCOL=TCPIP;",
            $config['driverName'],
            $config['database'],
            $config['host'],
            $config['port']
        );
    }
}
