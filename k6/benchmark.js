import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

export const options = {
    scenarios: {
        load_test: {
            executor: 'constant-vus',
            vus: 20,
            duration: '60s',
        },
    },
    thresholds: {
        http_req_duration: ['p(95)<500'],
        http_req_failed: ['rate<0.01'],
    },
};

const eventsPublished = new Counter('events_published');
const duplicatesSent = new Counter('duplicates_sent');

const AGGREGATOR_URL = __ENV.AGGREGATOR_URL || 'http://aggregator:8080';

export function setup() {
    console.log('Starting K6 benchmark...');
    console.log(`Target: ${AGGREGATOR_URL}`);

    const healthRes = http.get(`${AGGREGATOR_URL}/health`);
    if (healthRes.status !== 200) {
        throw new Error('Aggregator not healthy');
    }

    const statsRes = http.get(`${AGGREGATOR_URL}/stats`);
    console.log('Initial stats:', statsRes.body);

    return { startTime: Date.now() };
}

export default function () {
    const eventId = `k6-${__VU}-${__ITER}-${Date.now()}`;
    const isDuplicate = (__ITER % 10) < 3;

    const payload = JSON.stringify({
        events: [{
            event_id: isDuplicate ? `duplicate-vu-${__VU}` : eventId,
            topic: `benchmark-topic-${__VU % 5}`,
            source: 'k6-benchmark',
            payload: {
                message: 'K6 load test event',
                timestamp: new Date().toISOString(),
            },
            timestamp: new Date().toISOString(),
        }]
    });

    const res = http.post(`${AGGREGATOR_URL}/publish`, payload, {
        headers: { 'Content-Type': 'application/json' },
    });

    check(res, {
        'status is 200': (r) => r.status === 200,
        'has success status': (r) => r.json('status') === 'success',
    });

    eventsPublished.add(1);
    if (isDuplicate) {
        duplicatesSent.add(1);
    }

    sleep(0.01);
}

export function teardown(data) {
    sleep(5);

    const statsRes = http.get(`${AGGREGATOR_URL}/stats`);
    const stats = statsRes.json();

    const duration = (Date.now() - data.startTime) / 1000;

    console.log('\n========================================');
    console.log('          BENCHMARK RESULTS            ');
    console.log('========================================');
    console.log(`Duration:           ${duration.toFixed(2)}s`);
    console.log(`Events Received:    ${stats.received}`);
    console.log(`Unique Processed:   ${stats.unique_processed}`);
    console.log(`Duplicates Dropped: ${stats.duplicated_dropped}`);
    console.log(`Duplicate Rate:     ${((stats.duplicated_dropped / stats.received) * 100).toFixed(2)}%`);
    console.log(`Throughput:         ${(stats.received / duration).toFixed(2)} events/s`);
    console.log(`Topics:             ${stats.topics.join(', ')}`);
    console.log('========================================\n');
}
