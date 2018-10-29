const rethinkdbdash = require('rethinkdbdash');
const { MongoClient } = require('mongodb');

function log(...a) {
  console.log('[Migrator]', ...a);
}

class Migrator {
  constructor(config) {
    this.config = config;

    this.table = this.config.table;
  }

  async setupRethinkDB() {
    this.r = rethinkdbdash(this.config.rethinkdb);
  }

  async setupMongoDB() {
    this.mongoClient = await MongoClient.connect(this.config.mongodb);
    this.db = this.mongoClient.db();
  }

  async start() {
    await this.setupRethinkDB();
    await this.setupMongoDB();

    log('Getting document ids...');
    this.ids = await this.getDocumentIDs();
    log('Successfully got', this.ids.length, 'document ids');

    for(let id of this.ids) {
      await this.moveDocument(id);
    }
  }

  async getDocumentIDs() {
    let ids;
    if(this.config.conditions.user) {
      ids = await this.r.table(this.table).filter(
        this.r.row.hasFields('locale').or(this.r.row.hasFields('donatorMessage'))
      )
    } else {
      ids = await this.r.table(this.table).pluck('id').run();
    }

    return ids.map(d => d.id);
  }

  async moveDocument(id) {
    const res = await this.r.table(this.table).get(id);

    res._id = res.id;
    delete res.id;

    if(res._id instanceof Array) res._id = res._id.join('.');

    log('Moving document', res._id);

    await this.db.collection(this.table).insertOne(res).catch(console.log);
  }
}

const m = new Migrator(require('../config.json'));
m.start()