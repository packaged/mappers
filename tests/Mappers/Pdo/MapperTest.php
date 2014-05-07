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
    $id   = 5000;
    $user = User::load($id);
    $this->assertEquals($id, $user->id());

    $user->name        = rand();
    $user->description = rand();
    $user->save();
    $this->assertTrue($user->exists());

    $loadedUser = User::load($id);
    $this->assertTrue($loadedUser->exists());
    $this->compareObjects($user, $loadedUser);
  }

  public function testReload()
  {
    $user              = User::load(5000);
    $user->name        = 'reload' . rand();
    $user->description = 'reload' . rand();
    $user->reload();

    $this->compareObjects($user, User::load(5000));
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
