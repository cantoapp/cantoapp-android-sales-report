'use strict';

// Script para leer y procesar las transacciones por renovaciones
// de cantoapp tanto para android como para apple

// Dependencias
require('dotenv').config();
const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");
const csv = require('csv-parser');
const { default: axios } = require("axios");
const bluebird = require('bluebird');
const _ = require('lodash');
const moment = require('moment');
const Payment = require('./models/Payment');
const PartialPayment = require('./models/PartialPayment');

const findOne = (model, id, populates = []) => {
  return new Promise(function (resolve, reject) {
    try {
      let query = model.findOne({
        _id: new ObjectId(id)
      });
      if (populates && populates.length > 0) {
        populates.forEach(p => {
          query = query.populate(p);
        });
      }

      query.then((res) => {
        resolve(res);
      }).catch((err) => {
        //catch error
        if (err) reject(err);
      });
    } catch (err) {
      reject(err);
    }
  });
};


const findOneByTime = (model, field, populates = []) => {
  return new Promise(function (resolve, reject) {
    try {
      let query = model.findOne({
        platform: field
      },
        { sort: { datetime: -1 } },
        (err, data) => {
          console.log(data);
        },
      );
      if (populates && populates.length > 0) {
        populates.forEach(p => {
          query = query.populate(p);
        });
      }

      query.then((res) => {
        resolve(res);
      }).catch((err) => {
        //catch error
        if (err) reject(err);
      });
    } catch (err) {
      reject(err);
    }
  });
};

const find = (model, criteria, populates = []) => {
  return new Promise(function (resolve, reject) {
    try {
      let query = model.find(criteria);
      if (populates && populates.length > 0) {
        populates.forEach(p => {
          query = query.populate(p);
        });
      }
    

      query.then((res) => {
        resolve(res);
      }).catch((err) => {
        //catch error
        if (err) reject(err);
      });

    } catch (err) {
      reject(err);
    }
  });
};

const execute = (query) => {
  return new Promise(function (resolve, reject) {
    query.then((res) => {
      resolve(res);
    }).catch((err) => {
      //catch error
      if (err) reject(err);
    });

  });
};

const aggregate = (model, criteria) => {
  return new Promise(function (resolve, reject) {
    try {
      let query = model.aggregate(criteria);
      query.then((res) => {
        resolve(res);
      }).catch((err) => {
        //catch error
        if (err) reject(err);
      });

    } catch (err) {
      reject(err);
    }
  });
};

const findSort = (model, criteria, populates = []) => {
  return new Promise(function (resolve, reject) {
    try {
      let query = model.find(criteria).sort({ indexNumber: -1 });
      if (populates && populates.length > 0) {
        populates.forEach(p => {
          query = query.populate(p);
        });
      }
    
      query.then((res) => {
        resolve(res);
      }).catch((err) => {
        //catch error
        if (err) reject(err);
      });

    } catch (err) {
      reject(err);
    }
  });
};

// Renovaciones android

// Posibles mejoras
/* 
1-que se busque automatico el nombre del fichero en el bucket del mes anterior  y el año actual   / Pendiente de agregar


*/

// Paso 0
// Se borra el viejo reporte

// Credenciales del acceso al bucket

const { Storage } = require('@google-cloud/storage');
const storage = new Storage({ keyFilename: process.env.SERVICE_ACCOUNT_KEY_FILE });
// Función para la descarga desde el Bucket de Google
async function downloadFile(bucketName, fileName, destFilePath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(destFilePath);
    storage
      .bucket(bucketName)
      .file(fileName)
      .createReadStream()
      .on('error', (error) => {
        console.error('Error descargando el fichero:', error);
        reject(error);
      })
      .on('end', () => {
        console.log(`Fichero ${fileName} descargado en ${destFilePath}.`);
        resolve();
      })
      .pipe(file);
  });
}


// Lectura del csv de android
function readCsvFile(filePath) {
  return new Promise((resolve, reject) => {
    const results = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => resolve(results))
      .on('error', (error) => reject(error));
  });
}


// Inserción en mongo

const mongoose = require('mongoose');

// Define a schema
const renewalSchema = new mongoose.Schema({
  transactionDate: { type: String, required: true },
  transactionTime: { type: String, required: true },
  country: { type: String, required: true },
  transactionId: { type: String, required: true },
  transactionType: { type: String, required: true },
  productId: { type: String, required: true },
  currency: { type: String, required: true },
  amount: { type: String, required: true },
  googleFee: { type: String, required: true },
  conversionRate: { type: String, required: true },
  amountEUR: { type: String, required: true },
  basePlanId: { type: String, required: true },
  feeDescription: { type: String, required: true }
});

// Create a model
//const googlePaymentRenewal = mongoose.model('googlePaymentRenewal', renewalSchema);

// Conectando a mongodb
async function connectToDatabase() {
  try {
    const connect = await mongoose.connect(process.env.MONGODB_URL, {
    });
    return connect
    console.log('Connectado a MongoDB');
  } catch (error) {
    console.error('Error conectando a MongoDB:', error);
  }
}

// insertar el arreglo de renovaciones de google en mongo
async function insertgooglePaymentRenewal(googlePaymentRenewalArray, connection) {
  try {
    var conn = connection.connection
    const result = await conn.collection('googlePaymentRenewal').insertMany(googlePaymentRenewalArray);
    return result;
  } catch (error) {
    console.error('Error insertando documentos:', error);
    throw error;
  }
}

// Insertar los pagos parciales de las renovaciones
function insertPartialPayment(renewArray, connection) {
  try {
    if (renewArray != undefined) {
      var frecuency = 0;
      var plan = renewArray.basePlanId;
      var amount = renewArray.amount;
      var transaction = renewArray.transactionId;

      if (plan.includes("PREMIUM_12_AUTO")) {
        frecuency = 12;
      }
      if (plan.includes("PREMIUM_6_AUTO")) {
        frecuency = 6;
      }
      if (plan.includes("PREMIUM_3_AUTO")) {
        frecuency = 3;
      }
      // Se calcula el parcial
      const partialAmount = _.round(amount / frecuency);
      
      console.log(frecuency, partialAmount, plan, amount)
      
      // se crean los objetos para insertar los pagos parciales

      for (let i = 0; i < frecuency; i++) {
        const pDate = moment();
        if (i > 0) {
          pDate.add(i, 'months');
        }
        /*const partial = new PartialPayment({
          year: pDate.format('YYYY'),
          month: pDate.format('MM'),
          payment: newPayment._id,
          amount: partialAmount,
          owner: newPayment.user
        });*/

        const partialArray = {
          year: pDate.format('YYYY'),
          month: pDate.format('MM'),
          payment: transaction,
          amount: partialAmount,
          owner: "UNKNOW"
        };

        console.log(partialArray);
      }
      //await partial.save();
    }
    return true;
  } catch (error) {
    console.error('Error insertando documentos:', error);
    throw error;
  }
}

// Insertando renovaciones de apple
async function insertapplePaymentRenewal(applePaymentRenewalArray, connection) {
  try {
    var conn = connection.connection
    const result = await conn.collection('applePaymentRenewal').insertMany(applePaymentRenewalArray);
    return result;
  } catch (error) {
    console.error('Error insertando documentos:', error);
    throw error;
  }
}


// Funcion que hace todo el proceso de descargar el excel y copiarlo a mongo

async function descargaLecturaCSV() {
  // Datos de los ficheros y bucket en env
  // Se descarga el reporte del bucket con el nombre especificado
  try {

    // Se comprueba si el fichero se ha leido antes , se lee informacion del log

    var arrayLog = fs.readFileSync('./renewalLog.log', 'utf-8').split('\n')
    var lookForFile = arrayLog.find((element) => element == process.env.fileName)
    console.log(lookForFile);
    if (arrayLog.length == 0 || lookForFile == undefined) {
      // Paso 0
      // Se borra el viejo reporte
      fs.unlink(process.env.destFileName, (err) => {
        if (err) {
          console.error(err);
          return;
        }
        console.log('Reporte viejo borrado');
      });
      console.log(lookForFile);
      console.log('Descargando fichero...');
      const content = process.env.fileName + '\n';
      fs.appendFile('renewalLog.log', content, err => {
        if (err) {
          console.error(err);
        } else {
          // done!
        }
      });

      await downloadFile(process.env.bucketName, process.env.fileName, process.env.destFileName).catch(console.error)
      // Paso 1: Descarga de fichero
      console.log('Fichero descargado con éxito.');
      // Paso 2: Lectura del fichero descargado
      console.log('Lectura del csv descargado...');
      const data = await readCsvFile(process.env.destFileName);
      var i = 0
      /*
  Ejemplo del objeto que viene de android
  {
    Description: 'GPA.3306-6269-3448-78568',
    'Transaction Date': 'Dec 23, 2024',
    'Transaction Time': '1:20:34 PM PST',
    'Tax Type': '',
    'Transaction Type': 'Google fee',
    'Refund Type': '',
    'Product Title': 'Premium 12 meses (CantoApp)',
    'Product id': 'net.cantoappou.app',
    'Product Type': '1',
    'Sku Id': '09082024_premium12',
    Hardware: 'a15',
    'Buyer Country': 'FR',
    'Buyer State': '',
    'Buyer Postal Code': '49130',
    'Buyer Currency': 'EUR',
    'Amount (Buyer Currency)': '-4.00',
    'Currency Conversion Rate': '1.000000',
    'Merchant Currency': 'EUR',
    'Amount (Merchant Currency)': '-4.00',
    'Base Plan ID': '14082024-premium-12',
    'Offer ID': '',
    'Group ID': '4991888149209182909',
    'First USD 1M Eligible': 'Yes',
    'Service Fee %': '15',
    'Fee Description': 'SUBSCRIPTIONS',
    'Promotion ID': ''
  }
      */
     // Se crea el arreglo de mongo a partir de los objetos de pago de android
      var arrayMongoRebase = []
      while (i < data.length) {
        if (data[i]['Transaction Type'] == 'Charge') {
          var googleFee = ""
          var j = 0;
          while (j < data.length) {
            if (data[j]['Transaction Type'] == 'Google fee' && data[j]['Description'] == data[i]['Description']) {
              googleFee = data[j]['Amount (Merchant Currency)']
              break;
            }
            j++;
          }
          var objectMongo = {
            transactionDate: data[i]['Transaction Date'],
            transactionTime: data[i]['Transaction Time'],
            country: data[i]['Buyer Country'],
            transactionId: data[i]['Description'],
            transactionType: data[i]['Transaction Type'],
            productId: data[i]['Sku Id'],
            currency: data[i]['Buyer Currency'],
            amount: data[i]['Amount (Buyer Currency)'],
            googleFee: googleFee,
            conversionRate: data[i]['Currency Conversion Rate'],
            amountEUR: data[i]['Amount (Merchant Currency)'],
            basePlanId: data[i]['Base Plan ID'],
            feeDescription: data[i]['Fee Description'] == '' ? 'Suscription' : 'Renewal',
          }
          arrayMongoRebase.push(objectMongo)
        }
        i++;
      }
      // insertar los pagos parciales
      while (i < arrayMongoRebase.length) {
        console.log(arrayMongoRebase[i]);
        try {
          const result = insertPartialPayment(arrayMongoRebase[i], connection)
          console.log(result)
        } catch (error) {
          console.error('Error insertando:', error);
        }
        i++;
      }
      // Insercion de los objetos de pago por renovaciones en mongo
      const connection = await connectToDatabase();
      try {
        await insertgooglePaymentRenewal(arrayMongoRebase, connection);
      } catch (error) {
        console.error('Error insertando:', error);
      } finally {
        // Cerrando conex a mongo
        mongoose.connection.close();
      }
    
    }


  } catch (error) {
    console.error('Error:', error);
  }
}

// Llamada a la función que hace el proceso para android
descargaLecturaCSV()


// Renovaciones Apple
/*var urlTransactionsHistory = `https://validator.iaptic.com/v3/transactions`;
const encoded = Buffer.from('cantoapp:9f733aa3-77c6-4e15-9838-4a8166161c45').toString('base64');
const headers = { 'Authorization': ' Basic ' + encoded }; // auth header with bearer token
async function getHistoryTransactions(){
  const options = {
    method: "GET",
    url: urlTransactionsHistory,
    headers: {
      'Authorization': 'Basic ' + encoded
    } 
  };
  try {
    const { status, data = {} } = await axios(options);
    //console.log("RESPUESTA:",data);
    return data;
  } catch (err) {
    console.log("error", err);
    return "ERROR WHEN TRYING TO ACCESS TO PAYPAL SERVICE-[" + err + "]";
  }
};
const resultado = getHistoryTransactions();
resultado.then(function(result) {
  //console.log(result.rows)
  var arrayTransaction = result.rows;
  var i = 0;
  while (i < arrayTransaction.length) {
    if(arrayTransaction[i].sandbox!==true){
      console.log(arrayTransaction[i]);
    }       
  }
})*/


async function descargaLecturaCSVApple() {
  // Datos de los ficheros y bucket en env
  // Se descarga el reporte del bucket con el nombre especificado
  try {

    // Se comprueba si el fichero se ha leido antes , se lee informacion del log

    var arrayLog = fs.readFileSync('./renewalLogApple.log', 'utf-8').split('\n')
    var lookForFile = arrayLog.find((element) => element == process.env.fileNameApple)
    console.log(lookForFile);
    if (arrayLog.length == 0 || lookForFile == undefined) {
      // Paso 0
      // Se borra el viejo reporte
      fs.unlink(process.env.destFileNameApple, (err) => {
        if (err) {
          console.error(err);
          return;
        }
        console.log('Reporte viejo borrado');
      });
      console.log(lookForFile);
      console.log('Descargando fichero...');
      // se crea el log de lectura
      const content = process.env.fileNameApple + '\n';
      fs.appendFile('renewalLogApple.log', content, err => {
        if (err) {
          console.error(err);
        } else {
          // done!
        }
      });

      await downloadFile(process.env.bucketName, process.env.fileNameApple, process.env.destFileNameApple).catch(console.error)
      // Paso 1: Descarga de fichero
      console.log('Fichero descargado con éxito.');
      // Paso 2: Lectura del fichero descargado
      console.log('Lectura del csv descargado...');
      const readTextFile = _.partial(bluebird.promisify(fs.readFile), _, { encoding: 'utf8', flag: 'r' });
      const readJsonFile = filename => readTextFile(filename).then(JSON.parse);
      let data = await readJsonFile(process.env.destFileNameApple);
      var i = 0
      var arrayJson = data.rows
      const connection = await connectToDatabase();

    /* Ejemplo de objeto de pago por renovacion que se trae de iaptic
  {
     sandbox: false,
  platform: 'apple',
  productId: 'apple:PREMIUM_12_AUTO',
  purchaseId: 'apple:500001983017356',
  transactionId: 'apple:500000932966243',
  purchaseDate: '2024-12-14T02:11:13.000Z',
  lastRenewalDate: '2024-12-14T02:11:12.000Z',
  expirationDate: '2025-12-14T02:11:12.000Z',
  isIntroPeriod: false,
  quantity: 1,
  amountMicros: 29990000,
  currency: 'USD',
  renewalIntent: 'Renew',
  amountUSD: 29.99
  }
      */
      var arrayMongoRebase = []
      var compareFecha = new Date("2025-02-20");
      while (i < arrayJson.length) {
        var fechaPurchase = new Date(arrayJson[i].purchaseDate);
        console.log(fechaPurchase, compareFecha);
        if (arrayJson[i].sandbox == false && fechaPurchase.getTime() > compareFecha.getTime()) {

          var objectMongo = {
            transactionDate: arrayJson[i].purchaseDate,
            transactionTime: arrayJson[i].purchaseDate,
            country: "UNKNOW",
            transactionId: arrayJson[i].transactionId,
            transactionType: arrayJson[i].renewalIntent,
            productId: arrayJson[i].productId,
            currency: arrayJson[i].currency,
            amount: arrayJson[i].amountUSD,
            googleFee: 0,
            conversionRate: "UNKNOW",
            amountEUR: "UNKNOW",
            basePlanId: arrayJson[i].productId,
            feeDescription: arrayJson[i].renewalIntent == "" ? "Suscription" : "Renewal",
          }
          arrayMongoRebase.push(objectMongo)

        }
        i++;
      }
      i = 0;
      // se crean los objetos de pago parcial
      while (i < arrayMongoRebase.length) {
        console.log(arrayMongoRebase[i]);
        try {
          const result = insertPartialPayment(arrayMongoRebase[i], connection)
          console.log(result)
        } catch (error) {
          console.error('Error insertando:', error);
        }
        i++;
      }
      // Insercion de los pagos por renovaciones en la coleccion
      /*const connection = await connectToDatabase();
      try {
        await insertapplePaymentRenewal(arrayMongoRebase, connection);
      } catch (error) {
        console.error('Error insertando:', error);
      } finally {
        // Cerrando conex a mongo
        mongoose.connection.close();
      }*/
      //console.log('Contenido del csv:', data);
    }


  } catch (error) {
    console.error('Error:', error);
  }
}

// Llamada a la función que hace el proceso para apple
descargaLecturaCSVApple()