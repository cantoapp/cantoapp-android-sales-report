const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");

// Load the service account JSON key
const keyFile = path.join(__dirname, "service-account.json");

// Authenticate using Google API
async function authorize() {
    const auth = new google.auth.GoogleAuth({
        keyFile,
        scopes: ["https://www.googleapis.com/auth/androidpublisher"],
    });
    return await auth.getClient();
}

async function getPaymentsSummary(packageName, year, month) {
    const auth = await authorize();
    console.log(auth);
    const androidPublisher = google.androidpublisher({ version: "v3", auth });
    console.log(androidPublisher);

    try {
        /*const response = await androidPublisher.reports.generate({
            packageName,
            reportType: "earnings", // Report type: earnings, sales, etc.
            reportMonth: `${year}-${month}`,
            reportDimensions: ["transaction_type"], // Customize based on need
            format: "json"
        });*/

        const response = await androidPublisher.reports.search({
            packageName: packageName,
            filter: "finance",
        });

        console.log("Earnings Report:", JSON.stringify(response.data, null, 2));

        //console.log("Payments Summary:", response.data);
    } catch (error) {
        console.error("Error fetching payments summary:", error);
    }
}

authorize()
getPaymentsSummary("com.example.app", "2024", "01");

const { Storage } = require('@google-cloud/storage');
const fs = require('fs');
const path = require('path');

async function downloadFinancialReport() {
    const storage = new Storage({ keyFilename: 'tu-archivo-credenciales.json' });
    const bucketName = 'pubsite_prod_rev_XXXXXXXX/financial'; // Reemplaza con tu ID de desarrollador
    const fileName = 'financial_report_2024_01.csv'; // Reemplaza con el nombre real del archivo
    const destFileName = path.join(__dirname, fileName);

    try {
        await storage.bucket(bucketName).file(fileName).download({ destination: destFileName });
        console.log(`Reporte financiero descargado: ${destFileName}`);
    } catch (error) {
        console.error('Error al descargar el reporte financiero:', error);
    }
}

downloadFinancialReport();