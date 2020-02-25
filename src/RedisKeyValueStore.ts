import redisdown from 'redisdown'
import { LevelKeyValueStore } from '@cryptoeconomicslab/level-kvs'
import { Bytes } from '@cryptoeconomicslab/primitives'

export class RedisKeyValueStore extends LevelKeyValueStore {
  constructor(prefix: Bytes) {
    super(prefix, redisdown())
  }
}
