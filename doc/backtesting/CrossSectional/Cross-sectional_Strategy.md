# 横截面回测系统

## 目标

打造一个高效的易用的横截面策略的回测工具

## 概念 

横截面策略(Cross-sectional Strategy)是在某一个相同野战或者相对较短的时间窗口内，对一级投资标的进行横向比较，进而根据它们在该时点的特征差异来构建投资组合或者进行交易的一类方法。它关注的是不同标的在同一时间的相对表现，而非某个标的自身的时间序列趋势。

横截面策略回测系统的难点主要有以下几点：

- 多个标的数据的时间对齐
- 换月合约的处理

## 依赖

系统有两种数据

- **tick数据** - 是交易所中提供的最细粒度（最小时间间隔）的交易数据记录；每个数据包含：

  - 时间戳
  - 最新价格
  - 成交量
  - 买/卖盘报价
  - 持仓量
  - 等等

  它能较完整和连续地反映市场在微观层面的交易动态。

- **BAR数据** - 将市场交易信息按照一定的时间间隔或者其他维度（如成交量、笔数）进行聚合后得到的数据。最常见的是时间间隔聚合，即K线数据。BAR数据的主要特征如下：

  - **聚合维度**
    - 时间 - K线， 将固定时间区间的所有成交和报价信息合成一个BAR, 如1分钟的分钟线，5分钟线等）
    - 成交量： 当累计成交量达到一定数量后生成一个新的BAR，以捕捉不同时间段内市场活跃程度的变化
    - 笔数：当累计达到一定笔数的交易后生成的一个新BAR，主要判断交易笔数的时间长度分布
  - **包含信息** 
    - 开盘价(Open): 该BAR时间段内的第一笔成交价
    - 最高价(High): 该BAR时间段内的最高成交价
    - 最低价(Low): 该BAR时间内的最低价
    - 收盘价(Close): 该BAR时间内的最后一笔成交价
    - 成交量(Volume): 该BAR时间内的总成交量
    - 持仓量(OI): 该BAR时间收盘持仓量
    - 可能包含的：开盘持仓量，持仓变化等

回测系统先进行一个长期的测试，为了快速定位某些参数对策略的长期影响，可以使用数量小的BAR数据。使用tick数据，各个品种的数据量很大，在进行长时间的运算的时候达不到快速的要求。

## 规则

### 数据对齐

交易所提供的数据具有时间、频率、数据缺失或者错误等问题。针对此问题，
- 时间戳格式统一，不同的交易所提供的数据有精度或者时间格式差异；将数据采用统一的时间格式标准化；
- 数据格式差异，将不同的数据映射到标准的数据格式中；
- 因网络延迟或者数据同步机制的问题，导致不同的数据到达的时间不一样；需要将数据依照交易所的真实时间来重排
- 数据缺失值处理，根据上下文的值算出一个合理值进行填充
- 错误或者异常数据值处理
  - 删除异常值
  - 修正明显错误数值
  - 处理缺失时间段的数值

在数据对齐后，回测系统或者交易系统确保策略**等时等量**得到标准的数据。

### 换月规则

#### 主力合约

- 主力合约是在某一个特定商品或者指数的所有可交易期货合约中，成交量和持仓量（即未平仓的合约数）和流动性指标最高的合约。本文中的指标仅包含成交量和持仓量。

- 非主力合约是相对于主力合约来说，成交量、持仓量和流动性等指标较低的期货合约。

- 两者区别

  ---

  |    特征    | 主力合约 | 非主力合约 |
  | :--------: | :------: | :--------: |
  |   成交量   |    高    |     低     |
  |   持仓量   |    高    |     低     |
  |   流动性   |    高    |     低     |
  |  买卖价差  |   较小   |    较大    |
  | 价格波动性 |   较低   |    较高    |
  |  交割月份  |   接近   |    较远    |
  
---
  

- 主力合约指数值的计算公式 MI：

  $$
  MI = Vol \times W_v + OI \times W_{OI}

  W_v + W_{OI} = 1
  $$
  

- 在所有合约中，MI的值最大即为主力合约。

#### 基本原则

一旦非主力合约的指数值变成某一商品合约或者指数合约最大值后，即为主力合约，直到新的合约的MI值变成最大后。后期即使原主力合约的值恢复最大后，依然新选出的合约为主力合约。

### 产品价格指数

在期货交易中，我们通常使用**指数**(Index)来衡量某一特定产品或者一篮子产品的整体价格水平或者市场表现。在计算某一产品的指数，需要包含不同交割月份的合约。本文中确定产品指数的是包含所有的交易合约的加权指数。

#### 基本公式

常用的权重方法使用

- 成交量权重

  - 根据每个合约的成交量来分配权重，成交最大的合约权重更高

  - 公式：

$$
Index = \sum_{i = 1}^n \frac{Volume_i}{Volume_{total}}P_i

Volume_{total} = \sum_{i=1}^nVolume_i
$$
    

- 持仓权重 

  - 根据每个合约的持仓量（未平仓合约数）来分配权重，持仓量大的合约权重更高

  - 公式：

$$
Index = \sum_{i=1}^n \frac{OI_i}{OI_{total}}P_i

OI_{total}=\sum_{i=1}^nOI_i
$$
    

- 成交持仓混合权重

  - 假设成交量与持仓量的权重值分别为：W<sub>vol</sub>和W<sub>OI</sub> 

$$
W_{vol} + W_{OI} = 1
$$

  - 总的计算公式如下

$$
Index = W_{vol} \times \sum_{i=1}^n\frac{V_i}{V_{total}}Pi + W_{OI} \times \sum_{i=1}^n\frac{OI_i}{OI_{total}}Pi
$$
    

#### 合约滚动

期货合约具有到期日，当当前主力合约接近到期时，需要将其替换为下一个活跃合约。

#### 指数基准与标准化



## 基于日线的Cross-Sectional Strategy
### 日线数据结构
```
typedef struct bar
{
  uint32_t date;          /* date in int format, such as: 20250116 */
  double open;
  double high;
  double low;
  double close;
  double volume;
  double open_interest;
}
```

### 日线计算产品价格指数
假设当前交易所交易的行情，某个交易产品有5个合约，如contract_1, contract_2... contract_5, 计算各个数据字段的标准指数。使用成交持仓混合权重方式进行计算。

总成交量：
$$
volume_{total} = \sum_{i=1}^5contract_i.volume
$$

总持仓量：
$$
OI_{total} = \sum_{i=1}^5contract_i.open\_interest
$$

#### 开盘价
$$
open = W_{vol} \times \sum_{i=1}^5\frac{contract_i.volume}{volume_{total}} \times contract_i.open + W_{OI} \times \sum_{i=1}^5\frac{contract_i.open\_interest}{OI_{total}} \times contract_i.open
$$

#### 收盘价
$$
close = W_{vol} \times \sum_{i=1}^5\frac{contract_i.volume}{volume_{total}} \times contract_i.close + W_{OI} \times \sum_{i=1}^5\frac{contract_i.open\_interest}{OI_{total}} \times contract_i.close
$$

#### 最高价
$$
high = W_{vol} \times \sum_{i=1}^5\frac{contract_i.volume}{volume_{total}} \times contract_i.high + W_{OI} \times \sum_{i=1}^5\frac{contract_i.open\_interest}{OI_{total}} \times contract_i.high
$$

#### 最低价
$$
low = W_{vol} \times \sum_{i=1}^5\frac{contract_i.volume}{volume_{total}} \times contract_i.low + W_{OI} \times \sum_{i=1}^5\frac{contract_i.open\_interest}{OI_{total}} \times contract_i.low
$$

#### 成交量
$$
volume = \sum_{i=1}^5contract_i.volume
$$

#### 持仓量
$$
open\_interest = \sum_{i=1}^5contract_i.open\_interest
$$

### 可变参数
