# GatewayIQ — Get Started (Plain-English, Step-by-Step)

This guide is written for **someone with no technical background**. If you can
copy, paste, and fill in blanks, you can do this. Every term is explained the
first time it appears. Take it one numbered step at a time — don't skip ahead.

> **What is GatewayIQ?**
> It's a ready-made web dashboard that shows how your company is using AI
> (which teams, how much it costs, what's risky, etc.). This guide installs that
> dashboard into *your own* Databricks account so your people can log in and use it.
>
> **How long will this take?** About **45–90 minutes** the first time.
>
> **Do I need to write code?** No. You'll copy commands and fill in a few blanks.

---

## Choose your path (read this first)

There are **two ways** to install GatewayIQ. They do exactly the same thing —
pick whichever feels more comfortable:

- **🖱️ Path A — Do it all inside Databricks (no terminal).** You never leave your
  web browser. You copy the code into your workspace, open one page, type your
  settings into boxes, and click **Run**. **If terminals scare you, use this.**
  → Jump to **[Part A — Install inside the workspace](#part-a--install-inside-the-workspace-no-terminal)**.
- **⌨️ Path B — Use the Terminal on your computer.** You install two small tools,
  then run one command. Good if you like the command line or want to script it.
  → Continue with **Part 0** below and follow Parts 1–9.

Everything after this in the numbered Parts 1–9 is **Path B**. Path A is a single
self-contained section. You only need to read one path.

---

## Part A — Install inside the workspace (no terminal)

This whole path happens in your web browser, inside Databricks.

**Before you start,** get these from your Databricks admin (same list as Path B's
Part 1): a workspace login, **AI Gateway logging turned ON**, a **SQL warehouse
ID**, a **Lakebase instance name + host**, the app's **service-principal client id**,
and your **admin email(s)**.

1. **Copy the code into your workspace.** In the left sidebar click **Workspace**,
   then the **Create** button (top-right) → **Git folder**. In the box paste:
   `https://github.com/anuj1303/gatewayiq` and click **Create Git folder**. A folder
   called `gatewayiq` appears.
2. **Open the installer.** Inside the `gatewayiq` folder, open the **`scripts`**
   folder and click **`install_notebook`**. It opens like a page. Near the top is a
   single **config cell** — a block of `name: value` lines.
3. **Edit the config cell** with your answers from the checklist above:
   `warehouse_id`, `uc_catalog`, `inference_table`, `lakebase_instance`,
   `lakebase_host`, `app_sp`, `email_domain`, `admin_emails`. (It comes pre-filled
   with a real example you overwrite.) Leave `app_url` **blank** for now — you'll
   re-run once to fill it in later.
4. **Click "Run all"** at the top of the page. Wait — it prints its progress. When
   it finishes it shows a link: **your app's URL**.
5. **Open the app** (click the link), sign in with your company login (SSO), and add
   your people from the **Manage Users** tab.

That's it — no terminal, and **no secret scope to set up**. The installer also
created two background jobs (email + daily data refresh), both left **paused**; turn
them on from **Workflows** when you're ready (see the notes at the end of Part 7).

> **Email is self-service** — nothing to configure here. Once the app is running,
> each manager opens the **Notifications** tab and clicks **Connect Gmail** once, so
> their weekly reports send from *their own* address. (The first time, an admin also
> pastes the organization's Google OAuth client into that tab — a one-time step.)

> **Tip:** the notebook is safe to **Run all** again any time. After your first run,
> copy the app URL it printed into the `app_url` line and Run all once more so the
> in-app links point at the right place.

---

## Part 0 — Words you'll see (read this once)

You don't need to memorize these — just glance at them, then refer back.

| Word | What it actually means |
|---|---|
| **Databricks** | The cloud platform your company uses to store data and run analytics. GatewayIQ installs *inside* it. |
| **Workspace** | Your company's private area inside Databricks. It has a web address like `https://something.cloud.databricks.com`. |
| **Terminal** (or "command line") | A plain text window on your computer where you type commands instead of clicking buttons. On a Mac it's an app called **Terminal**; on Windows it's **Command Prompt** or **PowerShell**. |
| **Command** | A line of text you paste into the Terminal and press Enter to run. |
| **CLI** | "Command Line Interface" — a small program you install so your Terminal can talk to Databricks. |
| **Profile** | A saved login for the CLI, so you don't type your password every time. You give it a nickname (e.g. `myprofile`). |
| **Warehouse** | The engine inside Databricks that runs the data queries. You just need its ID (a string of letters/numbers). |
| **Lakebase** | A fast database inside Databricks that the dashboard reads from. The installer creates it for you. |
| **Service Principal (SP)** | A "robot user" — an identity the app logs in as (instead of a person). Your Databricks admin creates one. |
| **Secret scope** | A locked box inside Databricks for passwords/keys, so they're never written in plain files. |
| **YAML** | A simple text file format for settings. You'll edit one file (`customer.yaml`); it's just `name: value` lines. |
| **Repo / repository** | This folder of files (the GatewayIQ code) — the thing you're reading right now. |

---

## Part 1 — Before you start (the checklist)

You need a few things ready. **If you don't have these, ask your Databricks
administrator** — they'll know exactly what these mean. Don't try to create them
yourself unless you're the admin.

Tick each box before moving on:

- [ ] **A Databricks workspace** you can log into (the web address + your login).
- [ ] **Admin help available.** Some steps need an admin. Line them up in advance.
- [ ] **AI Gateway logging is turned ON.** This is what produces the data the
      dashboard shows. Ask your admin: *"Is Unity AI Gateway usage tracking and
      inference (payload) logging enabled, and are system tables on?"* If they say
      no, they need to turn it on first — **stop here until they do.**
- [ ] **A SQL Warehouse exists** (ask the admin for its **ID**).
- [ ] **A Lakebase instance exists** (ask the admin for its **name** and **host**).
- [ ] **A Service Principal for the app** (ask the admin for its **client id** —
      a long code like `1111-2222-3333`).
- [ ] **The email addresses of your admin(s)** — the people who should manage the
      system. That's all you need for identity; you add everyone else from inside
      the app once it's running. *(No org directory table required.)*
- [ ] *(Optional)* An existing org directory table (email, team, manager, title)
      if you'd rather bulk-import your people once instead of adding them by hand.
- [ ] *(Nothing needed for email now.)* Weekly-report email is set up **inside the
      app** afterward — each manager connects their own mailbox. See Part 5.

> 💡 **Tip:** Copy all the answers above into a notes file (Apple Notes, Notepad,
> anything). You'll paste them into one config file in Part 4.

---

## Part 2 — Install the two tools you need

You need two free programs on your computer: **Python** and the **Databricks CLI**.

### 2a. Open your Terminal
- **Mac:** Press `Cmd + Space`, type `Terminal`, press Enter.
- **Windows:** Press the Start button, type `PowerShell`, press Enter.

A window with text and a blinking cursor opens. This is where you'll paste commands.

### 2b. Check if Python is already installed
Paste this and press Enter:
```bash
python3 --version
```
- If you see something like `Python 3.11.x` → ✅ you have it, skip to 2c.
- If you see an error → download Python from **https://www.python.org/downloads/**,
  run the installer, click "Next" through it (on Windows, **check the box that says
  "Add Python to PATH"**), then close and reopen your Terminal and try the command
  again.

### 2c. Install the Databricks CLI
Paste this and press Enter (this installs the tool that talks to Databricks):
```bash
pip3 install databricks-cli pyyaml
```
Wait for it to finish (a minute or two). Then confirm it worked:
```bash
databricks --version
```
If you see a version number, ✅ you're good.

---

## Part 3 — Connect the CLI to your Databricks (log in once)

This saves your Databricks login under a nickname (a "profile") so the installer
can do its work. Pick any nickname — this guide uses **`myprofile`**.

Paste this, replacing the web address with **your** workspace's address:
```bash
databricks configure --token --profile myprofile --host https://YOUR-WORKSPACE.cloud.databricks.com
```

It will ask for a **token** (a temporary password). To get one:
1. Open your Databricks workspace in a web browser and log in.
2. Click your **name/photo** in the top-right → **Settings**.
3. Go to **Developer** → **Access tokens** → **Generate new token**.
4. Give it a name (e.g. "gatewayiq install"), click Generate, and **copy** the
   long code it shows. (You won't see it again, so copy it now.)
5. Go back to your Terminal, **paste** the code, and press Enter.

Test that it worked:
```bash
databricks current-user me --profile myprofile
```
If it prints your name/email, ✅ you're connected.

---

## Part 4 — Get the code, then fill in your settings

First you download the GatewayIQ code onto your computer, then you make your own
copy of a settings template and fill in the blanks with your Part 1 notes.

### 4a. Download (clone) the GatewayIQ code

**Option A — with `git` (recommended).** Most computers already have `git`. In the
Terminal, paste:
```bash
git clone https://github.com/anuj1303/gatewayiq.git
```
This downloads a folder named `gatewayiq` into wherever your Terminal currently is
(usually your home folder). If it says `command not found: git`, use Option B.

**Option B — no `git` (download a ZIP).** Open
**https://github.com/anuj1303/gatewayiq** in your browser → green **Code** button →
**Download ZIP** → double-click the downloaded file to unzip it. You'll get a
folder called `gatewayiq-main`.

### 4b. Go into the GatewayIQ folder
In the Terminal, type `cd ` (with a space), then **drag the folder you just got
(`gatewayiq` or `gatewayiq-main`) from your file explorer into the Terminal
window** (this pastes its location), then press Enter. It looks like:
```bash
cd /Users/you/gatewayiq
```
To confirm you're in the right place, type `ls` and press Enter — you should see
`customer.yaml.example`, `install.sh`, and a `scripts` folder listed.

### 4c. Make your own copy of the settings template
```bash
cp customer.yaml.example customer.yaml
```
This creates `customer.yaml` — **this is the file you edit.** (Never edit the
`.example` one; it's the blank master.)

### 4d. Open `customer.yaml` in a text editor
- **Mac:** `open -e customer.yaml`
- **Windows:** `notepad customer.yaml`

You'll see lines like `name: value` with `#` comments explaining each one. Replace
the example values (they mention a fake company "Acme") with **your** values from
your Part 1 notes. The important ones:

| Line in the file | What to put |
|---|---|
| `profile:` | your nickname from Part 3 → `myprofile` |
| `warehouse_id:` | the Warehouse ID from your admin |
| `lakebase: instance / host` | the Lakebase name and host from your admin |
| `lakebase: admin_user` | **your own** Databricks login email |
| `lakebase: app_sp` | the Service Principal client id from your admin |
| `uc: catalog` | where the data tables get created (ask admin, or use the example) |
| `sources: inference_table` | the AI Gateway logging table from your admin |
| `sources: directory_table` | **leave commented out** unless you're bulk-importing an existing directory |
| `identity: email_domain` | your company email domain, e.g. `acme.com` |
| `identity: admins` | the emails of people who should see **everything** and manage users (usually you) |
| `identity: managers` | leave as `[]` — you add managers from inside the app. Only fill this to pre-create managers before anyone logs in. |
| `app: url` | leave the example for now; you can update it after first deploy |

**Save the file** (Mac: `Cmd+S`; Windows: File → Save) and close the editor.

> 💡 Don't worry about the `model_pricing` block — leave it commented out. The
> installer figures out AI pricing automatically.

---

## Part 5 — Email (set up later, inside the app)

**There is nothing to do here at install time**, and no "secret scope" to create.
Email is **self-service inside the app** once it's running, so the weekly reports
send from **each manager's own mailbox** — not a shared address.

When you're ready for email (after the app is up), two one-time steps happen in the
app's **Notifications** tab:

1. **An admin connects the organization's Google account (once).** In Google Cloud,
   someone creates an OAuth "Web application" client and registers the app's
   callback address (`https://<your-app-url>/api/gmail/oauth/callback`). The admin
   pastes that client's **id + secret** into the panel on the Notifications tab.
   *(Ask whoever manages your company's Google/Workspace — this is a normal Google
   admin task.)*
2. **Each manager clicks "Connect Gmail" (once).** They approve a Google popup, and
   from then on their weekly reports send from their own address. They can
   disconnect any time.

So: you can finish the install now and set up email whenever you like — no keys, no
locked box, no re-deploy.

---

## Part 6 — Run the installer (the big moment)

This one command does everything: it sets up the app, creates the database, loads
your data, and figures out AI pricing. Make sure you're still inside the
GatewayIQ folder (from step 4b), then paste:

```bash
./install.sh customer.yaml
```

Press Enter and **wait**. It prints its progress in 4 steps:
1. `resolve per-model pricing…` — works out AI costs from your billing.
2. `render app.yaml…` — turns your settings into the app's config.
3. `databricks bundle deploy…` — installs the app and its database into Databricks.
4. `data-plane install…` — loads your data and sets up who-can-see-what.

This can take several minutes. When it finishes you'll see:
```
✅ GatewayIQ installed. Open the app (SSO); Notifications → send a test; resume the weekly Job when ready.
```

> **If it stops with a red error:** don't panic. Copy the **last 15–20 lines** of
> text and see Part 8 (Troubleshooting), or send them to whoever set this up. The
> installer is **safe to run again** — fix the one setting it complained about in
> `customer.yaml` and re-run `./install.sh customer.yaml`. It won't create
> duplicates.

---

## Part 7 — Open your dashboard and check it works

1. In your Databricks workspace (web browser), click **Compute** → **Apps** in the
   left menu (wording may vary slightly). You'll see an app named **gatewayiq**.
2. Click it, then click the **URL / "Open app"** link. It opens in a new tab.
3. It logs you in automatically with your Databricks account (this is "SSO" —
   single sign-on, no separate password).
4. What you should see:
   - You land on a **My Usage** page.
   - Because you listed yourself as an **admin** in `customer.yaml`, you'll see
     **all the tabs** across the top (Executive Overview, Users & Teams, Anomaly
     Detection, and so on).
   - Charts and numbers fill in. 🎉 **You're done.**

**Add your people.** Open the **Manage Users** tab. Fill in the short form —
**Name**, **Email**, choose a **Manager**, and pick a **Role** (User, Manager, or
Admin) — then click **Add user**. Repeat for everyone who needs access. It works
just like adding users to a Databricks workspace. Users see only their own AI
usage, managers see their team, admins see everyone. You can change anyone's role
or manager later from the same tab.

To turn on email (Part 5): open the **Notifications** tab, click **Connect Gmail**
(an admin sets the org Google client there first, once), then **Send test to
myself** to confirm delivery from your own address.

> **Turning on the weekly email (later, when you're ready):** the installer created
> a weekly email job but left it **paused** and in **test mode** (all mail goes to
> one test address) so it can't accidentally email your whole company. When you're
> confident, ask your admin to flip `test_mode` to `false` and un-pause the job.

> **Keeping the dashboard up to date:** the install loads your data once. The
> installer also created a **"GatewayIQ — Data Refresh"** job that rebuilds the
> data from your live AI Gateway tables (daily, by default) so the dashboard stays
> current — but it's left **paused** too. Once you've confirmed the numbers look
> right, open **Workflows**, find that job, and un-pause it. It costs a little each
> run (it re-classifies requests with AI); if you'd rather avoid that, set
> `data_refresh.skip_classifier: true` in `customer.yaml` and re-deploy. Until you
> un-pause it, the dashboard keeps showing the data from install time.

---

## Part 8 — If something goes wrong (Troubleshooting)

| What you see | What it usually means | What to do |
|---|---|---|
| `command not found: databricks` | The CLI didn't install, or Terminal needs restarting. | Close and reopen Terminal; redo Part 2c. |
| `command not found: git` | You don't have `git` installed. | Use **Option B** in step 4a — download the ZIP from the GitHub page instead. |
| `config not found: customer.yaml` | You're not in the right folder, or didn't copy the template. | Redo steps 4b and 4c. |
| Error mentioning **profile** or **401 / unauthorized** | Your login token expired or the profile name is wrong. | Redo Part 3 (get a fresh token); check `profile:` in `customer.yaml` matches. |
| Error mentioning **warehouse** | Wrong or missing Warehouse ID. | Double-check `warehouse_id:` with your admin. |
| Error mentioning **permission / CREATE / role** | Your login can't create something; an admin needs to grant access. | Send the error to your Databricks admin. |
| Error mentioning a **table** or **schema** not found | A source table name is wrong. | Re-check the `sources:` lines with your admin. |
| The app opens but is **empty / no charts** | Data didn't load, or logging wasn't on. | Confirm AI Gateway logging is ON (Part 1), then re-run `./install.sh customer.yaml`. |

**Golden rule:** the installer is *idempotent* — a fancy word meaning **you can
safely run it as many times as you want**. Fix one thing, run it again. Nothing
breaks from re-running.

---

## Part 9 — Where to get more detail

- `README.md` — the same process written for engineers (more technical, terser).
- **Part A** above — the no-terminal path, if you'd rather do everything inside the
  Databricks web app (`scripts/install_notebook`).
- The `# comments` inside `customer.yaml.example` — explain every single setting.
- Your Databricks admin — the right person for anything about accounts,
  permissions, warehouses, or service principals.

---

*You did it. If you got stuck on a specific step, note the step number — that makes
it much faster for anyone to help you.*
