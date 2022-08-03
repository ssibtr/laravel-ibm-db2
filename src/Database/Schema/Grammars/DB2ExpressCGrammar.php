<?php

namespace GuidoFaecke\DB2\Database\Schema\Grammars;

class DB2ExpressCGrammar extends DB2Grammar
{
    /**
     * Compile the query to determine the list of tables.
     *
     * @return string
     */
    public function compileTableExists(): string
    {
        return 'select * from syspublic.all_tables where table_schema = upper(?) and table_name = upper(?)';
    }

    /**
     * Compile the query to determine the list of columns.
     *
     * @return string
     */
    public function compileColumnExists(): string
    {
        return 'select column_name
                from syspublic.all_ind_columns
                where table_schema = upper(?) and table_name = upper(?)';
    }
}
