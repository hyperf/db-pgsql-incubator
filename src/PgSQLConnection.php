<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
namespace Hyperf\DB\PgSQL;

use Closure;
use Hyperf\DB\AbstractConnection;
use Hyperf\Pool\Pool;
use Psr\Container\ContainerInterface;
use Swoole\Coroutine\PostgreSQL;

class PgSQLConnection extends AbstractConnection
{
    /**
     * @var PostgreSQL
     */
    protected $connection;

    /**
     * @var array
     */
    protected $config = [
        'driver' => PgSQLPool::class,
        'host' => '127.0.0.1',
        'port' => 5432,
        'database' => 'hyperf',
        'username' => 'root',
        'password' => '',
        'pool' => [
            'min_connections' => 1,
            'max_connections' => 32,
            'connect_timeout' => 10.0,
            'wait_timeout' => 3.0,
            'heartbeat' => -1,
            'max_idle_time' => 60.0,
        ],
    ];

    public function __construct(ContainerInterface $container, Pool $pool, array $config)
    {
        parent::__construct($container, $pool);
        $this->config = array_replace_recursive($this->config, $config);
        $this->reconnect();
    }

    public function reconnect(): bool
    {
        $connection = new PostgreSQL();
        $connection->connect(
            sprintf('host=%s port=%s dbname=%s user=%s password=%s'),
            $this->config['host'],
            $this->config['port'],
            $this->config['database'],
            $this->config['username'],
            $this->config['password']
        );

        $this->connection = $connection;
        $this->lastUseTime = microtime(true);
        $this->transactions = 0;
        return true;
    }

    public function close(): bool
    {
        unset($this->connection);

        return true;
    }

    public function insert(string $query, array $bindings = []): int
    {
        $query = rtrim($query, ' ;') . ' RETURNING id;';

        $statement = $this->prepare($query);

        $result = $this->connection->execute($statement, $bindings);
        $arr = $this->connection->fetchRow($result);
        var_dump($arr);

        return $statement->insert_id;
    }

    public function execute(string $query, array $bindings = []): int
    {
        // TODO: Implement execute() method.
    }

    public function exec(string $sql): int
    {
        // TODO: Implement exec() method.
    }

    public function query(string $query, array $bindings = []): array
    {
        $result = $this->connection->query($query, $bindings);
        $arr = $pg->fetchAll($result);
    }

    public function fetch(string $query, array $bindings = [])
    {
        // TODO: Implement fetch() method.
    }

    public function call(string $method, array $argument = [])
    {
        // TODO: Implement call() method.
    }

    public function run(Closure $closure)
    {
        // TODO: Implement run() method.
    }

    protected function prepare(string $query): string
    {
        $id = uniqid();

        $this->connection->prepare($id, $query);

        return $id;
    }
}
