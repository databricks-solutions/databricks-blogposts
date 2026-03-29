# From the Data Lakehouse to Your Local Workstation: Understanding Your Images with AI

## The use case

Many teams keep unstructured data, such as images, in Databricks Unity Catalog Volumes. Some of these teams specifically want to work from their local workstation so they can run fast, hands-on analysis without waiting on shared compute environments. These files are valuable, but hard to use at scale unless they can be automatically described and organized.

These users need a secure way to access those images from a local workstation, then apply local AI models on that same workstation to classify and caption them. The outcome is simple and actionable: each image gets labels and a plain-language description, which helps teams search, review, and manage visual data faster. This is useful for content operations, analytics, and compliance workflows where both speed and governance matter.

## The user journey

The journey starts when a user authenticates to Databricks using OAuth (U2M). Databricks then uses credential vending to issue temporary, limited-scope credentials for the Unity Catalog volume. In practice, this means the user can read only what is needed for the task, for a limited time, without exposing long-lived storage secrets.

With that secure access in place, the image files are downloaded from the Unity Catalog volume to a local working directory. The local AI step then runs image classification and image captioning on those files. The final output is a clean summary of visual content that teams can use immediately for discovery, triage, and reporting.

## Who this is for

This workflow is for data and analytics teams, content managers, ML practitioners, and platform teams that manage unstructured data in Unity Catalog. It fits both one-time analysis and repeatable operational use, especially when teams need strong access controls and fast insight generation from image data.

## Example output

Running `python run_download_and_process.py` downloads images from Unity Catalog and processes them locally. Here is a sample run with two images:

### Sample images

<table>
  <tr>
    <td align="center">
      <img src="https://github.com/user-attachments/assets/9d2be70a-b9a0-43b8-bc20-7b9920907887" width="400"><br>
      <sub>Bliss_(Windows_XP).png</sub>
    </td>
    <td align="center">
      <img src="https://github.com/user-attachments/assets/15be835f-37c0-44ad-b7df-53e48202324e" width="400"><br>
      <sub>flower.jpg</sub>
    </td>
  </tr>
</table>

### Classification (google/vit-base-patch16-224)

**Bliss_(Windows_XP).png**
- monitor: 47.72%
- desktop computer: 23.70%
- screen, CRT screen: 15.74%
- notebook, notebook computer: 3.12%
- television, television system: 1.20%

**flower.jpg**
- chambered nautilus, pearly nautilus, nautilus: 16.47%
- daisy: 8.51%
- pot, flowerpot: 1.55%
- coil, spiral, volute, whorl, helix: 1.36%
- ear, spike, capitulum: 0.92%

### Captioning (Salesforce/blip-image-captioning-base)

| Image | Caption |
|-------|---------|
| Bliss_(Windows_XP).png | a green hill with a blue sky |
| flower.jpg | a flower with a blury background |