import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { parse } from "csv-parse";
import fs from "fs";
import axios from "axios";
const REGION = "us-east-1";
const s3Client = new S3Client({ region: REGION });
const BUCKET = "mylawcle.com";
const START_FROM = 11;

// Create the Amazon S3 bucket.
export const upload = async (fileName, folder) => {
  console.log("Uploading " + folder + "/" + fileName);

  const parallelUploads3 = new Upload({
    client: s3Client,
    params: {
      Bucket: BUCKET,
      Body: fs.createReadStream(fileName),
      Key: folder + "/" + fileName,
    },
  });

  parallelUploads3.on("httpUploadProgress", (progress) => {
    console.log(
      "Upload progress " +
        folder +
        "/" +
        fileName +
        " " +
        Math.round((progress.loaded / progress.total) * 100 * 100) / 100 +
        "%"
    );
  });

  await parallelUploads3.done();

  console.log("Upload completed " + folder + "/" + fileName);
};

export const download = async (url, fileName) => {
  console.log("Downloading " + fileName);

  const writer = fs.createWriteStream(fileName);
  const response = await axios.get(url, { responseType: "stream" });
  response.data.pipe(writer);

  return new Promise((resolve, reject) => {
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
};

const processFile = async () => {
  let records;
  records = [];
  const parser = fs
    .createReadStream("./list.csv")
    .pipe(parse({ delimiter: ",", from_line: 2 }));
  for await (const record of parser) {
    records.push(record);
  }
  return records;
};

const records = await processFile();

let count = 0;
for (const row of records) {
  count++;

  if (count < START_FROM) {
    continue;
  }
  console.log("Record " + count + " of " + records.length + ":");

  const productId = row[2];
  const title = row[3];
  const videoLink = row[4];
  const resourceLink = row[5];

  const videoUrl = new URL(videoLink);
  const videoPath = videoUrl.pathname;
  const videoFileName = videoPath.substring(videoPath.lastIndexOf("/") + 1);

  await download(videoLink, videoFileName);
  await upload(videoFileName, productId);
  fs.unlinkSync(videoFileName);

  if (resourceLink) {
    const resourceUrl = new URL(resourceLink);
    const resourcePath = resourceUrl.pathname;
    const resourceFileName = resourcePath.substring(
      resourcePath.lastIndexOf("/") + 1
    );

    await download(resourceLink, resourceFileName);
    await upload(resourceFileName, productId);
    fs.unlinkSync(resourceFileName);
  }
}
