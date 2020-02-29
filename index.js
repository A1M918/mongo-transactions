const Mongo = require('mongo-models');

let client = {};

class MongoModel extends Mongo {
  static async connectDb() {
    console.log('Connecting DB', {
      uri: process.env.MONGO_URI,
      db: process.env.MONGO_DB,
    });
    if (Object.keys(client).length > 0) {
      console.log('db already connected');
      return client;
    }
    client = await this.connect(
      {
        uri: process.env.MONGO_DB_KRAKEN,
        db: process.env.MONGO_DB,
      },
      { writeConcern: 'majority' },
    );
    return Mongo.clients.default;
  }

  /**
   * @static
   * @returns {session} The Start session client
   * @memberof MongoModel
   */
  static async getSession() {
    console.log('Starting Session');
    // await client.connect();
    const connection = await this.connectDb();
    const session = await connection.startSession();
    return session;
  }

  /**
   * @static
   * @param {*} session
   * @returns
   * @memberof MongoModel
   */
  static async startTransaction(session) {
    console.log('Starting transaction');
    return session.startTransaction();
  }

  static async commitTransaction(session) {
    try {
      await session.commitTransaction();
      console.log('session ended');
      await session.endSession();
      console.log('transaction committed');
      return true;
    } catch (error) {
      await this.abortTransaction(session);
      throw error;
    }
  }

  static async abortTransaction(session) {
    await session.abortTransaction();
    await session.endSession();
  }
}

module.exports = MongoModel;
