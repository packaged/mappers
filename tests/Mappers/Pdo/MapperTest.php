<?php

/**
 * Created by PhpStorm.
 * User: tom.kay
 * Date: 15/04/2014
 * Time: 14:44
 */
class MapperTest extends PHPUnit_Framework_TestCase
{
  public function compareObjects($obj1, $obj2)
  {
    foreach($obj1 as $k => $v)
    {
      $this->assertEquals($obj1->$k, $obj2->$k);
    }
    foreach($obj2 as $k => $v)
    {
      $this->assertEquals($obj2->$k, $obj1->$k);
    }
  }

  public function testNew()
  {
    $user              = new User();
    $user->name        = rand();
    $user->description = rand();
    $user->save();
    $id = $user->id();
    $this->assertNotEmpty($id);
    $this->compareObjects($user, User::load($id));
    $user->name        = rand();
    $user->description = rand();

    $user = $user->saveAsNew();
    $this->assertNotSame($id, $user->id());
    $this->compareObjects($user, User::load($user->id()));
  }

  public function testLoad()
  {
    $user              = new User();
    $user->name        = 'name' . rand() . time();
    $user->description = 'desc' . rand() . time();
    $user->save();

    $loaded = User::load($user->id());
    $this->assertEquals($user->id(), $loaded->id());
    $this->compareObjects($user, $loaded);

    $user->name        = rand();
    $user->description = rand();
    $user->save();
    $this->assertTrue($user->exists());

    $loadedUser = User::load($user->id());
    $this->assertTrue($loadedUser->exists());
    $this->compareObjects($user, $loadedUser);
  }

  public function testReload()
  {
    $user              = new User();
    $user->name        = 'name' . rand() . time();
    $user->description = 'desc' . rand() . time();
    $user->save();

    $user->name        = 'reload' . rand();
    $user->description = 'reload' . rand();
    $user->reload();

    $this->compareObjects($user, User::load($user->id()));
  }

  public function testDelete()
  {
    $user              = new User();
    $user->name        = 'testDelete';
    $user->description = rand();
    $user->save();
    $id = $user->id();
    $this->compareObjects($user, User::load($id));

    $user->delete();

    $deleted = User::load($id);
    $this->assertFalse($deleted->exists());
    $this->compareObjects($deleted, new User());
  }
}
