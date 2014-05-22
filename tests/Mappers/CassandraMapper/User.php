<?php

class CassUser extends \Packaged\Mappers\CassandraMapper
{
  public $id;
  public $name;
  public $description;
  public $countField = 0;

  public function keyField()
  {
    return 'id';
  }


  public static function getTableName()
  {
    return 'cass_users';
  }

}
