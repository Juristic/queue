import { Redis } from 'ioredis'
import { RedisAdapter } from '../src/drivers/redis_adapter.js'

const SCHEDULE_COUNT = 100
const CLAIM_ROUNDS = 3
const KEY_PREFIX = 'boringnode::queue::bench::'

async function run() {
  const connection = new Redis({
    host: '127.0.0.1',
    port: 6379,
    db: 15,
    keyPrefix: KEY_PREFIX,
  })

  await connection.flushdb()

  const adapter = new RedisAdapter(connection)
  adapter.setWorkerId('bench-worker')

  console.log(`Creating ${SCHEDULE_COUNT} schedules...`)

  const createStart = performance.now()
  for (let i = 0; i < SCHEDULE_COUNT; i++) {
    await adapter.upsertSchedule({
      id: `bench-schedule-${i}`,
      name: `BenchJob${i}`,
      payload: { index: i },
      everyMs: 60_000,
      timezone: 'UTC',
    })
  }
  console.log(`Created in ${(performance.now() - createStart).toFixed(0)}ms`)

  for (let round = 1; round <= CLAIM_ROUNDS; round++) {
    const dueAt = Date.now() - 1000
    const pipeline = connection.pipeline()
    for (let i = 0; i < SCHEDULE_COUNT; i++) {
      pipeline.hset(
        `schedules::bench-schedule-${i}`,
        'next_run_at',
        dueAt.toString()
      )
      pipeline.zadd('schedules::due', dueAt, `bench-schedule-${i}`)
    }
    await pipeline.exec()

    console.log(`\n--- Round ${round}: claiming ${SCHEDULE_COUNT} due schedules ---`)

    const claimStart = performance.now()
    let claimed = 0

    while (true) {
      const schedule = await adapter.claimDueSchedule()
      if (!schedule) break
      claimed++
    }

    const elapsed = performance.now() - claimStart

    console.log(`Claimed: ${claimed}`)
    console.log(`Total:   ${elapsed.toFixed(1)}ms`)
    console.log(`Per claim: ${(elapsed / claimed).toFixed(2)}ms`)
  }

  await connection.flushdb()
  await connection.quit()
}

run().catch((err) => {
  console.error(err)
  process.exit(1)
})
