<?php

use Packaged\Mappers\CassandraMapper;

class IndexedMapper extends CassandraMapper
{
  public $id;
  public $testData;
  /**
   * @index
   */
  public $indexedField;

  /**
   * @index my_custom_index
   */
  public $customIndexedField;
}
 