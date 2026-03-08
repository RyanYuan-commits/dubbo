/**
 * Remoting 层提供客户端与服务端通信的功能
 * 包含 Exchange, Transport, Serialize 三个子层级
 * - Exchange 层封装请求响应模式, 同步转异步, 核心是 Response 和 Request;
 * - Transport 层抽象 netty 等网络框架为统一接口, 核心是 Message;
 * - Serialize 层封装一些可复用的工具.
 */
package org.apache.dubbo.remoting;
