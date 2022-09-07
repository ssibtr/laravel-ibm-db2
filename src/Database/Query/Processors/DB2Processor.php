<?php

namespace SystemServices\DB2\Database\Query\Processors;

use SystemServices\DB2\Database\Query\Grammars\DB2Grammar;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;

/**
 * Class DB2Processor
 *
 * @package SystemServices\DB2\Database\Query\Processors
 */
class DB2Processor extends Processor
{
    /**
     * Process an "insert get ID" query.
     *
     * @param Builder $query
     * @param string $sql
     * @param array  $values
     * @param string|array $sequence
     *
     * @return int|array
     */
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null)
    {
        $sequenceStr = $sequence ?: 'id';

        if (is_array($sequence)) {
            $grammar = new DB2Grammar();
            $sequenceStr = $grammar->columnize($sequence);
        }

        $sqlStr = 'select %s from new table (%s)';

        $finalSql = sprintf($sqlStr, $sequenceStr, $sql);
        $results = $query->getConnection()
                         ->select($finalSql, $values);

        if (is_array($sequence)) {
            return array_values((array) $results[0]);
        }
        $result = (array) $results[0];
        $id = $result[$sequenceStr] ?? $result[strtoupper($sequenceStr)];

        return is_numeric($id) ? (int) $id : $id;
    }
}
