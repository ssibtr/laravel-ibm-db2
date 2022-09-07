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
        return "ibm:{$config['database']}";
    }
}
