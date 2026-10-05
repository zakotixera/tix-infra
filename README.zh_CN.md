# Tix 基础组件

**用于 Python 的配置管理、有界并发、状态机和资源生命周期管理。**

[English](README.md) | 简体中文

Tix infrastructure 包含四个相互独立、带类型标注的 Python 包，要求 Python 3.11 或更高版本。
各包可单独使用，也可在应用中组合使用。运行时状态仅作用于各自的实例，
各包之间没有依赖关系。四个包均包含 `py.typed` 标记，支持类型检查。

| 包 | 导入名 | 主要功能 | 运行时依赖 |
| --- | --- | --- | --- |
| [tix-config](tix-config/README.md) | `tix_config` | 分层配置、不可变快照、校验和原子重载 | 无必需依赖；可选 PyYAML |
| [tix-asyncio](tix-asyncio/README.md) | `tix_asyncio` | 有界任务、工作池、重试、重复请求合并和取消 | AnyIO |
| [tix-transition](tix-transition/README.md) | `tix_transition` | 可复用状态机定义、同步/异步执行和检查点 | transitions、AnyIO |
| [tix-lifecycle](tix-lifecycle/README.md) | `tix_lifecycle` | 按依赖顺序启动、回滚、资源释放和健康检查 | AnyIO |

## 目录

- [安装](#安装)
- [基本用法](#基本用法)
- [执行与资源管理](#执行与资源管理)
- [示例](#示例)
- [开发](#开发)
- [文档](#文档)
- [许可证](#许可证)

## 安装

在应用的虚拟环境中，从仓库根目录按需安装：

```sh
python -m pip install ./tix-config
python -m pip install ./tix-asyncio
python -m pip install ./tix-transition
python -m pip install ./tix-lifecycle
```

每条命令安装一个独立的发行包及其运行时依赖。发行包名使用连字符，Python 导入名使用下划线。
如需读取 YAML 配置，安装可选依赖：

```sh
python -m pip install "./tix-config[yaml]"
```

也可通过 `python -m pip install path/to/package.whl` 安装已构建的 wheel 文件。
以上命令使用本地源码安装。仓库根目录是开发工作区，不是可安装的聚合包。
开发环境的配置方法见[开发](#开发)。

## 基本用法

安装对应的包后，以下示例均可独立运行。

### 分层配置

合并默认配置与应用覆盖项，并获取不可变快照：

```python
from tix_config import ConfigManager, MappingSource

config = ConfigManager(
    [
        MappingSource({"db": {"host": "localhost", "port": 5432}}, name="defaults"),
        MappingSource({"db": {"port": 6543}}, name="overrides"),
    ]
)
config.reload()
snapshot = config.snapshot
assert snapshot.at("db.host") == "localhost"
assert snapshot.at("db.port") == 6543
```

后面的配置源优先，映射中的键递归合并。每次操作开始时获取一次快照，即可读取一致的配置版本。
配置源加载或校验失败时，旧快照保持不变；订阅回调的错误则发生在新快照发布之后。

TOML/JSON/YAML 文件、环境变量、敏感值、校验和重载通知的用法见
[配置指南](tix-config/README.md)。

### 有界并发任务

通过背压控制任务提交，等待任务作用域内的工作完成后读取结果：

```python
import anyio
from tix_asyncio import TaskManager


async def square(value: int) -> int:
    await anyio.lowlevel.checkpoint()
    return value * value


async def main() -> None:
    async with TaskManager(concurrency=2) as tasks:
        handles = [await tasks.submit(square, value) for value in range(5)]
    print([await handle.result() for handle in handles])


anyio.run(main)
```

输出为 `[0, 1, 4, 9, 16]`。`submit()` 在创建子任务之前等待可用容量。
正常退出作用域时会等待已接收的任务完成；默认错误策略会取消同组的其他任务并向上传播错误。
管理器会立即释放对已完成任务句柄的持有，但调用方保留结果仍会占用内存。

工作任务就绪通知、流式并发映射、重试策略、取消令牌和 singleflight 的用法见
[异步工具指南](tix-asyncio/README.md)。

### 可复用状态机

定义工作流，并通过命名事件推进状态：

```python
from tix_transition import MachineDefinition, State, Transition, TransitionManager

definition = MachineDefinition(
    states=(State("draft"), State("approved"), State("sent", final=True)),
    transitions=(
        Transition("approve", "draft", "approved"),
        Transition("send", "approved", "sent"),
    ),
    initial="draft",
    name="message",
    version="1",
)
manager = TransitionManager(definition, history_limit=16)
assert manager.dispatch("approve").accepted
assert manager.dispatch("send").accepted
assert manager.is_final
```

定义不可变，可供多个独立的管理器复用。守卫条件与回调可访问应用上下文和事件载荷。
检查点只保存工作流状态，不会持久化或重放应用的副作用。

异步分派、回调阶段、结构编辑和原生层级状态机的用法见
[状态机指南](tix-transition/README.md)。

### 资源生命周期

声明依赖，使资源按顺序启动，并在退出作用域时释放：

```python
import anyio
from tix_lifecycle import Component, LifecycleContext, LifecycleManager


async def open_store(context: LifecycleContext) -> dict[str, str]:
    return {"status": "ready"}


async def close_store(context: LifecycleContext, store: dict[str, str]) -> None:
    store.clear()


async def open_service(context: LifecycleContext) -> dict[str, str]:
    return context.get("store")


async def main() -> None:
    manager = LifecycleManager(
        [
            Component("store", open_store, close_store),
            Component("service", open_service, requires=("store",)),
        ]
    )
    async with manager:
        store = manager.get("service")
        assert store["status"] == "ready"
    assert store == {}


anyio.run(main)
```

同一依赖层内的独立组件并发启动，清理按依赖的逆序执行。启动失败时，会释放已成功获取的资源；
启动钩子若在返回资源前失败，必须自行清理尚未交给管理器的资源。

超时、健康探测、资源所有权、ASGI lifespan 和信号处理的用法见
[生命周期指南](tix-lifecycle/README.md)。

## 执行与资源管理

依赖关系和资源生命周期由应用显式管理：

| 关注点 | 行为约定 |
| --- | --- |
| 配置读取 | 获取的快照不可变。重载是同步操作；在异步应用中，应将阻塞的配置源 I/O 移至工作线程，例如使用 `anyio.to_thread.run_sync(config.reload)`。 |
| 异步后端 | 托管异步工具和生命周期操作通过 AnyIO 支持 asyncio 与 Trio。在开发工作区之外选择 Trio 时，需单独安装 Trio。asyncio 专用的原生导出仍要求使用 asyncio。 |
| 异步状态机 | `AsyncTransitionManager` 要求使用 asyncio，与上游 `AsyncMachine` 后端一致。带类型标注的定义描述扁平状态机。 |
| 原生 API | `tix-asyncio` 和 `tix-transition` 直接导出上游 API。原生任务不会自动归属于 Tix 管理器；原生状态机保留上游行为。 |
| 任务所有权 | 任务管理器为一次性实例，必须在同一任务中进入和退出。任务作用域应嵌套在拥有其所需资源的生命周期作用域内。 |
| 资源所有权 | 生命周期管理器为一次性实例。启动钩子返回所拥有的资源，停止钩子负责释放。托管异步对象仅属于一个事件循环。 |
| 取消与超时 | 取消是协作式的。超时无法强制终止阻塞操作或忽略取消的钩子。信号处理需要显式启用。 |
| 工作流错误 | 回调错误向上传播，状态取决于执行到的阶段；应用的外部副作用不会自动回滚。 |

原生 API 的行为以及托管 API 提供的额外保证，见[上游兼容性说明](docs/upstream-compatibility.md)。

## 示例

[配置好开发工作区](#开发)后，可运行：

| 示例 | 命令 | 演示内容 |
| --- | --- | --- |
| [服务生命周期](examples/service.py) | `uv run python examples/service.py` | 配置校验、资源依赖、有界任务、工作流转换、健康检查和清理 |
| [上游 API](examples/upstream_apis.py) | `uv run python examples/upstream_apis.py` | 原生层级状态机、带类型标注的守卫条件、工作任务就绪通知和流式结果 |

两个示例均会自行结束。依赖安装完成后，运行时无需数据库、网络服务或凭据。
服务示例组合了全部四个包，并在释放资源前结束使用这些资源的任务作用域。

## 开发

安装 `uv` 后，在仓库根目录执行：

```sh
uv sync --locked --all-packages
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run python scripts/typecheck.py
uv run python examples/service.py
uv run python examples/upstream_apis.py
uv build --all-packages
uv run python scripts/check_wheels.py
```

`uv.lock` 记录共享的依赖解析结果。`--locked` 检查锁文件是否与工作区元数据一致。
类型检查脚本根据构建元数据中的包名映射运行严格模式的 mypy，
避免将 `tix-asyncio/src/asyncio` 与标准库模块混淆。

构建产物（源码分发包和 wheel）位于 `dist/`。wheel 检查脚本会分别在隔离环境中安装各包，
检查公开导入和类型标记。默认使用 Python 3.11；可通过 `--python 3.13` 等参数选择其他解释器。

[CI](.github/workflows/ci.yml) 配置覆盖 Linux、Windows 和 Python 3.11–3.14，
包括覆盖率门槛为 90% 的测试、代码检查、格式检查、类型检查、示例、构建和隔离 wheel 安装。
异步工具和生命周期测试同时覆盖 asyncio 与 Trio；异步状态机测试使用 asyncio。
已记录的检查结果和平台限制见[验证记录](docs/validation.md)。

运行本地性能基准测试：

```sh
uv run python benchmarks/run.py --iterations 10000 --repeats 5
```

[基准测试指南](benchmarks/README.md) 说明了测试负载和测量限制。
结果仅作为本地基线，不保证实际应用的性能，也不用于证明优于上游库。
CI 中没有依赖特定机器性能的耗时阈值。

## 文档

以下详细指南目前为英文：

| 指南 | 内容 |
| --- | --- |
| [配置管理](tix-config/README.md) | 配置源优先级、敏感值、快照、校验、重载和持久化 |
| [异步工具](tix-asyncio/README.md) | 任务所有权、背压、就绪通知、并发映射、重试和取消 |
| [状态机](tix-transition/README.md) | 定义、守卫条件、回调、原生变体和检查点 |
| [生命周期](tix-lifecycle/README.md) | 依赖图、清理、健康检查、ASGI lifespan 和信号适配器 |
| [架构](docs/architecture.md) | 设计约定、参考研究、扩展点和尚未实现的功能 |
| [上游兼容性](docs/upstream-compatibility.md) | 原生 API 行为与托管扩展 |
| [验证记录](docs/validation.md) | 已记录的检查结果、运行环境和平台限制 |
| [基准测试](benchmarks/README.md) | 本地测量、测试方法和结果解读 |

## 许可证

[Apache License 2.0](LICENSE)。
