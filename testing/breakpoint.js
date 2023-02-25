import http from 'k6/http';
import {check} from 'k6';

export const options = {
  scenarios: {
    my_scenario1: {
      executor: 'constant-arrival-rate',
      duration: '10s', // total duration
      preAllocatedVUs: 10, // to allocate runtime resources     preAll

      rate: 100, // number of constant iterations given `timeUnit`
      timeUnit: '1s',
    },
  },
};

var num = 1;

export default function () {
    num++;
    const headers = { 'Content-Type': 'application/json' };
    const res = http.get(`http://34.222.134.132/reviews/?product_id=${num}`, { headers });
    check(res, {
        'is status 200': (r) => r.status === 200,
    });
}