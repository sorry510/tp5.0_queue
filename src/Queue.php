<?php
// +----------------------------------------------------------------------
// | ThinkPHP [ WE CAN DO IT JUST THINK IT ]
// +----------------------------------------------------------------------
// | Copyright (c) 2006-2015 http://thinkphp.cn All rights reserved.
// +----------------------------------------------------------------------
// | Licensed ( http://www.apache.org/licenses/LICENSE-2.0 )
// +----------------------------------------------------------------------
// | Author: yunwuxin <448901948@qq.com>
// +----------------------------------------------------------------------

namespace think;

use RedisException;
use think\helper\Str;
use think\queue\Connector;

/**
 * Class Queue
 * @package think\queue
 *
 * @method static push($job, $data = '', $queue = null)
 * @method static later($delay, $job, $data = '', $queue = null)
 * @method static pop($queue = null)
 * @method static marshal()
 */
class Queue
{
    /** @var Connector */
    protected static $connector;

    private static function buildConnector()
    {
        if (!isset(self::$connector)) {
            self::createConnector();
        }
        return self::$connector;
    }

    private static function createConnector()
    {
        $options = Config::get('queue');
        $type = !empty($options['connector']) ? $options['connector'] : 'Sync';
        $class = false !== strpos($type, '\\') ? $type : '\\think\\queue\\connector\\' . Str::studly($type);
        self::$connector = new $class($options);
        return self::$connector;
    }

    private static function isRedisConnectException($message)
    {
        if (Str::contains($message, 'went away')) {
            return true;
        }
        if (Str::contains($message, 'errno=10054')) {
            // 服务器端主动断开连接
            return true;
        }
        return false;
    }

    public static function __callStatic($name, $arguments)
    {
        try {
            return call_user_func_array([self::buildConnector(), $name], $arguments);
        } catch (RedisException $e) {
            if (self::isRedisConnectException($e->getMessage())) {
                self::createConnector();
            }
            throw $e;
        }
    }
}
