// Dependencias
require('dotenv').config();
const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");
const csv = require('csv-parser');

// Paso 0
// Se borra el viejo reporte

fs.unlink(process.env.destFileName, (err) => {
  if (err) {
    console.error(err);
    return;
  }
  console.log('Reporte viejo borrado');
});

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
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    return connect
    console.log('Connectado a MongoDB');
  } catch (error) {
    console.error('Error conectando a MongoDB:', error);
  }
}

// Insert an array of data
async function insertgooglePaymentRenewal(googlePaymentRenewalArray, connection) {
  try {
    var conn = connection.connection
    const result = await conn.collection('googlePaymentRenewal').insertMany(googlePaymentRenewalArray);
    //console.log(`${result.length} documents inserted`);
    return result;
  } catch (error) {
    console.error('Error insertando documentos:', error);
    throw error;
  }
}

/*// Main function
async function mainInsertion() {
  // Connect to the database
  await connectToDatabase();

  // Array of users to insert
  const usersArray = [
    { name: 'John', age: 25, city: 'New York' },
    { name: 'Jane', age: 30, city: 'Los Angeles' },
    { name: 'Alice', age: 28, city: 'Chicago' },
  ];

  // Insert the array of users
  try {
    await insertgooglePaymentRenewal(usersArray);
  } catch (error) {
    console.error('Error in main function:', error);
  } finally {
    // Close the database connection
    mongoose.connection.close();
  }
}

*/
// Funcion que hace todo el proceso de descargar el excel y copiarlo a mongo

async function descargaLecturaCSV() {
  // Datos de los ficheros y bucket en env
  // Se descarga el reporte del bucket con el nombre especificado
  try {
    
    // Se comprueba si el fichero se ha leido antes , se lee informacion del log

    var arrayLog = fs.readFileSync('./renewalLog.log', 'utf-8').split('\n')
    var lookForFile = arrayLog.find((element) => element == process.env.fileName)
    console.log(lookForFile);
    if(arrayLog.length==0 || lookForFile==undefined){
         console.log(lookForFile);
         console.log('Descargando fichero...');
         const content = process.env.fileName+'\n';
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
         /*const connection = await connectToDatabase();
         try {
           await insertgooglePaymentRenewal(arrayMongoRebase, connection);
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

// Llamada a la función que hace el proceso
descargaLecturaCSV()

