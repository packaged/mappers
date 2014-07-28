<?php

/**
 * @Entity
 * @Table(name="static_mappers",indexes={@Index(name="primary_key",columns={"id2"})})
 */
class StaticMapper extends \Packaged\Mappers\CassandraMapper
{
  /**
   * @Id
   */
  public $id;
  public $id2;
  /**
   * @static
   */
  public $myStatic;
  public $nonStatic;
}
 