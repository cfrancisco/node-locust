const chance = require('chance')();

const { Locust, TaskSet } = require('./locust');
const { HTTPClient } = require('./clients/http');
const logger = require('./logger');

const config = {
  host: 'localhost',
  port: '8000'
};

if (!config.host || !config.port) {
  logger.error('Env variable required: API_HOST, API_PORT');
}


class MqttUser extends TaskSet {
  constructor() {
    super();

    this.http = new HTTPClient(
      {
        baseURL: `${config.host}:${config.port}`,
        timeout: 2000
      }, this.recordSuccess.bind(this),
      this.recordFailure.bind(this)
    );

    this.tasks = [
      {
        task: () => this.getPage(),
        weight: 1
      }
    ];
  }

  before() { console.log('before'); return ' '; }
  //after() { console.log('after'); return ' '; }


  async getPage() {
    await this.http.request({
      method: 'GET',
      url: '/#/'
    });
  }
}


class MqttProject extends Locust {
  constructor() {
    super();

    this.taskSet = new MqttUser();
  }
}

module.exports = { MqttProject };

