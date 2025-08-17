const express = require("express");
const multer = require("multer");
const { handleUpload } = require("../controllers/uploadController");

const router = express.Router();

const storage = multer.diskStorage({
  destination: "uploads",
  filename: (_, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname.replace(/\s+/g, "_"));
  },
});
const upload = multer({ storage });

router.post(
  "/metrics",
  upload.fields([{ name: "files", maxCount: 10 }]),
  handleUpload
);

module.exports = router;
