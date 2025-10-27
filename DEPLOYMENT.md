# Deployment Guide: makbo.satisa.be

**Target**: Vercel deployment with custom domain `makbo.satisa.be`
**Status**: Ready for deployment
**Last Updated**: 2025-10-27

---

## Quick Reference

- **Production URL**: https://makbo.satisa.be
- **Repository**: https://github.com/stevenn/NewAgeKBO
- **Framework**: Next.js 15 (App Router)
- **Node Version**: 20.x
- **Database**: Motherduck (cloud DuckDB)

---

## Step-by-Step Deployment

### 1️⃣ Clerk Setup (15 minutes)

**Create Production Application:**

1. Go to https://dashboard.clerk.com
2. Click "Add application"
3. Name: `Modern Age KBO` (or `MAKBO Production`)
4. Select authentication methods:
   - ✅ Email + Password (recommended)
   - Optional: Social providers (Google, etc.)
5. Click "Create application"

**Get Production Keys:**

After creation, go to **API Keys**:
```bash
CLERK_SECRET_KEY=sk_live_xxxxxxxxxxxxxxxxxxxxx
NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_live_xxxxxxxxxxxxxxxxxxxxx
```
⚠️ **Save these securely** - you'll need them for Vercel

**Configure Session Claims:**

1. Navigate to **Sessions** → **Customize session token**
2. Add this JSON to the session token template:
```json
{
  "metadata": "{{user.public_metadata}}"
}
```
3. Save changes

**Configure Domains:**

1. Go to **Domains**
2. Add frontend domain: `makbo.satisa.be`
3. Save

**Create First Admin User:**

1. Go to **Users** → **Create user**
2. Add email and password (or use your existing account)
3. After creation, click the user → **Metadata** tab
4. Add to **Public metadata**:
```json
{
  "role": "admin"
}
```
5. Save

---

### 2️⃣ Vercel Project Setup (10 minutes)

**Import from GitHub:**

1. Go to https://vercel.com/dashboard
2. Click "Add New" → "Project"
3. Import Git Repository → Select `stevenn/NewAgeKBO`
4. Configure project:
   - **Framework Preset**: Next.js (auto-detected)
   - **Root Directory**: `./` (leave default)
   - **Build Command**: `npm run build`
   - **Output Directory**: `.next` (leave default)
   - **Install Command**: `npm ci`
   - **Node.js Version**: 20.x

**⚠️ STOP - Don't click "Deploy" yet!**
First, add environment variables below.

---

### 3️⃣ Environment Variables (10 minutes)

In Vercel Project Settings → **Environment Variables**, add:

#### Required Variables

| Variable | Value | Environment | Notes |
|----------|-------|-------------|-------|
| `MOTHERDUCK_TOKEN` | `your_md_token` | Production | From Motherduck dashboard |
| `MOTHERDUCK_DATABASE` | `kbo` | Production | Database name |
| `CLERK_SECRET_KEY` | `sk_live_...` | Production | From Clerk (Step 1) |
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | `pk_live_...` | Production | From Clerk (Step 1) |
| `CRON_SECRET` | Generate new | Production | See below |
| `KBO_USERNAME` | Your KBO username | Production | For future cron jobs |
| `KBO_PASSWORD` | Your KBO password | Production | For future cron jobs |
| `NODE_ENV` | `production` | Production | Auto-set by Vercel |
| `NEXT_PUBLIC_APP_URL` | `https://makbo.satisa.be` | Production | Custom domain |

#### Generate CRON_SECRET

Run this in your terminal:
```bash
openssl rand -hex 32
```
Copy the output and paste as `CRON_SECRET` value.

**After adding all variables:**
- Click "Deploy" or trigger deployment

---

### 4️⃣ Custom Domain Setup (15-60 minutes)

**In Vercel:**

1. Project Settings → **Domains**
2. Add domain: `makbo.satisa.be`
3. Vercel will show required DNS records

**In your DNS provider (satisa.be):**

Add CNAME record:
```
Type: CNAME
Name: makbo
Value: cname.vercel-dns.com  (or value from Vercel)
TTL: 3600
```

**Wait for DNS propagation:**
- Usually 5-30 minutes
- Check with: `dig makbo.satisa.be`
- Vercel will auto-provision SSL (Let's Encrypt)

---

### 5️⃣ Verification Checklist

After deployment completes and DNS propagates:

#### Basic Functionality
- [ ] Visit `https://makbo.satisa.be` → Landing page loads
- [ ] Click "Sign in" → Redirects to Clerk
- [ ] Sign in with admin user → Redirects to `/admin/dashboard`
- [ ] Dashboard shows statistics (proves Motherduck connection works)
- [ ] No console errors in browser DevTools

#### Admin Features
- [ ] Dashboard loads with stats and charts
- [ ] Browse page shows enterprise list
- [ ] Search by enterprise number works
- [ ] Search by name works
- [ ] Enterprise detail page loads
- [ ] Temporal navigation (snapshots) works
- [ ] Import jobs page shows history
- [ ] Settings page displays config

#### Security
- [ ] `/admin/*` requires authentication (redirects to sign-in)
- [ ] Non-admin users see "Unauthorized" page
- [ ] HTTPS works (SSL certificate valid)
- [ ] HTTP redirects to HTTPS

#### Performance
- [ ] First load < 3 seconds (cold start)
- [ ] Subsequent loads < 500ms
- [ ] Search results load quickly

---

### 6️⃣ Post-Deployment Configuration

**Optional but recommended:**

#### Clerk Configuration
- Set up email templates for password reset
- Configure OAuth providers if needed
- Set up webhooks for user events (if needed)

#### Vercel Settings
- **Analytics**: Already integrated with Next.js
- **Web Vitals**: Check in Vercel Analytics tab
- **Deployment Protection**: Consider password-protecting preview deployments
- **Security Headers**: Review in Project Settings

#### Monitoring
- Check Vercel logs for errors: Project → Logs
- Set up alerts for deployment failures
- Monitor Motherduck usage/costs

---

## Troubleshooting

### Deployment Fails

**Build Error: "Cannot find module 'duckdb'"**
- Already handled by `next.config.ts` (duckdb externalized)
- Should not occur

**Build Error: Missing environment variables**
- Check all required variables are set in Vercel
- Ensure `NEXT_PUBLIC_*` variables are set for Production

**Timeout during build**
- Increase build timeout in Vercel settings (default: 15min)
- Check for hanging processes

### DNS/Domain Issues

**Domain not resolving**
- Wait longer (can take up to 1 hour)
- Check DNS with `dig makbo.satisa.be +short`
- Verify CNAME points to Vercel

**SSL Certificate error**
- Vercel auto-provisions after DNS propagates
- Usually takes 2-5 minutes after DNS resolves
- Check in Vercel Domains tab

### Runtime Errors

**500 Error on all pages**
- Check Vercel logs (Project → Logs)
- Verify `MOTHERDUCK_TOKEN` is correct
- Verify `CLERK_SECRET_KEY` is correct

**Authentication not working**
- Verify Clerk keys are production keys (sk_live_/pk_live_)
- Check `makbo.satisa.be` is in Clerk allowed domains
- Verify session claims are configured

**Database queries fail**
- Check Motherduck token is valid
- Verify database name is `kbo`
- Ensure views are created (`scripts/create-views.ts`)

**"Unauthorized" for admin user**
- Verify user metadata has `"role": "admin"` in Clerk
- Check session claims configuration (Step 1)
- Try signing out and back in

---

## Rollback Procedure

If something goes wrong:

1. Go to Vercel → **Deployments**
2. Find last working deployment
3. Click "..." → **Promote to Production**
4. Previous version goes live immediately
5. No DNS changes needed

---

## Environment Variable Reference

### Public Variables (exposed to browser)
- `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` - Clerk public key
- `NEXT_PUBLIC_APP_URL` - Application URL

### Private Variables (server-only)
- `CLERK_SECRET_KEY` - Clerk secret key
- `MOTHERDUCK_TOKEN` - Database connection token
- `MOTHERDUCK_DATABASE` - Database name
- `KBO_USERNAME` - KBO portal username
- `KBO_PASSWORD` - KBO portal password
- `CRON_SECRET` - Cron job authentication
- `NODE_ENV` - Environment mode

---

## Next Steps After Deployment

### Phase 4 (Automation)
- [ ] Set up Vercel Cron for daily updates
- [ ] Automate monthly full imports
- [ ] Implement 24-month retention policy

### Security Improvements
- [ ] Fix SQL injection vulnerabilities (parameterized queries)
- [ ] Add authentication to API routes
- [ ] Add rate limiting

### Documentation
- [ ] Update IMPLEMENTATION_GUIDE.md with deployment info
- [ ] Document production architecture
- [ ] Create runbook for common operations

---

## Support Resources

- **Vercel Docs**: https://vercel.com/docs
- **Clerk Docs**: https://clerk.com/docs
- **Next.js Docs**: https://nextjs.org/docs
- **Motherduck Docs**: https://motherduck.com/docs
- **Project Issues**: https://github.com/stevenn/NewAgeKBO/issues

---

## Deployment History

| Date | Version | Changes | Deployed By |
|------|---------|---------|-------------|
| TBD  | Initial | Phase 3 complete | - |

