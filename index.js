import { S3Client, HeadObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { parse } from "csv-parse";
import { stringify } from "csv-stringify/sync";
import fs from "fs";
import axios from "axios";
const REGION = "us-east-1";
const s3Client = new S3Client({ region: REGION });
const BUCKET = "mylawcle.com";
const START_FROM = 0;

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

export const fileExists = async (fileName, folder) => {
  const response = await s3Client.send(
    new HeadObjectCommand({
      Bucket: BUCKET,
      Key: folder + "/" + fileName,
    })
  );

  return response["$metadata"].httpStatusCode === 200;
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
let newData = [];
for (const row of records) {
  count++;

  if (count < START_FROM) {
    continue;
  }
  console.log("Record " + count + " of " + records.length + ":");

  try {
    const productId = row[2];
    const title = row[3];
    const videoLink = row[4];
    const resourceLink = row[5];

    let newRecord = {};
    newRecord.title = title;

    if (videoLink && videoLink.toLowerCase() !== "na") {
      const videoUrl = new URL(videoLink);
      const videoPath = videoUrl.pathname;
      const videoFileName = videoPath.substring(videoPath.lastIndexOf("/") + 1);

      if (!fileExists(videoFileName, productId)) {
        await download(videoLink, videoFileName);
        await upload(videoFileName, productId);
        fs.unlinkSync(videoFileName);
      } else {
        console.log("File exists " + productId + "/" + videoFileName);
      }
      newRecord.video_1_link =
        "https://s3.amazonaws.com/" +
        BUCKET +
        "/" +
        productId +
        "/" +
        videoFileName;
    }

    if (resourceLink && resourceLink.toLowerCase() !== "na") {
      const resourceUrl = new URL(resourceLink);
      const resourcePath = resourceUrl.pathname;
      const resourceFileName = resourcePath.substring(
        resourcePath.lastIndexOf("/") + 1
      );

      if (!fileExists(resourceFileName, productId)) {
        await download(resourceLink, resourceFileName);
        await upload(resourceFileName, productId);
        fs.unlinkSync(resourceFileName);
      } else {
        console.log("File exists " + productId + "/" + resourceFileName);
      }
      newRecord.video_1_resource_1 =
        "https://s3.amazonaws.com/" +
        BUCKET +
        "/" +
        productId +
        "/" +
        resourceFileName;
    }
    newData.push(newRecord);
  } catch (e) {
    console.error(e.message);
  }
}

fs.writeFileSync("result.csv", stringify(newData, { header: true }));
