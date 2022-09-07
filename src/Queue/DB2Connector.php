<?php

namespace SystemServices\DB2\Queue;

use Illuminate\Contracts\Queue\Queue;
use Illuminate\Queue\Connectors\DatabaseConnector;

class DB2Connector extends DatabaseConnector
{
    /**
     * Establish a queue connection.
     *
     * @param  array  $config
     * @return Queue
     */
    public function connect(array $config)
    {
        return new DB2Queue(
            $this->connections->connection($config['connection'] ?? null),
            $config['table'],
            $config['queue'],
            $config['retry_after'] ?? 60
        );
    }
}
