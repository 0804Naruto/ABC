        public async Task<string> PayslipReport(BackgroundEmail myQueItem, string SchemaName, int companyId, string userId, bool Filter, List<int> FilterIds, string payslipPath)
        {
            var timer = new Stopwatch();
            timer.Start();
            string employeeid = "0";
            List<string> PayslipsSchemas = new List<string> {
                    "Smollfe37426078",
                    "MGMHEe93284675e",
                    "Orionb8f410a437",
                    "Total4cd8b47020",
                    "BPOINfa194f1362"
                };
            try
            {
                myQueItem.PayslipLogId = 0;
                myQueItem.MailSendBool = false;
                List<PayslipFailedMailLog> ListFailedLog = new List<PayslipFailedMailLog>();
                iTextSharp.text.Font fntTableFont = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 6, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                iTextSharp.text.Font fntTableFontBold = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 7, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                iTextSharp.text.Font fntTableFontKOB = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 6, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                iTextSharp.text.Font fntTableFontKOBBold = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 6, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                iTextSharp.text.Font fntTableFontComp = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 9, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                iTextSharp.text.Font fntTableFontCompBold = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 9, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                iTextSharp.text.Font fntTableFontCompBold11 = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 11, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                iTextSharp.text.Font fntTableFontCompBold88 = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 8, iTextSharp.text.Font.BOLD, BaseColor.BLACK);

                fntTableFont = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 8, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                fntTableFontBold = FontFactory.GetFont(BaseFont.TIMES_ROMAN, 8, iTextSharp.text.Font.BOLD, BaseColor.BLACK);

                List<EmpTransaction> empTranscData = new List<EmpTransaction>();
                bool stopPay = myQueItem.stopPay;
                string Msg = string.Empty;
                string SubmitType = myQueItem.SubmitType;
                int maxcounter = 0;
                int cnt = 0;
                //if (SubmitType == "Send Email")
                //    context = GlobalHost.ConnectionManager.GetHubContext<PayrollHub>();
                ReceiversEmail recEmail;
                int countEmpEmail = 0;
                int countEmpWhatsapp = 0;
                string EmpsReceivedMail = string.Empty;
                string EmpsNotReceivedMail = string.Empty;
                string EmpsReceivedWhatsApp = string.Empty;
                string EmpsNotReceivedWhatsApp = string.Empty;
                string EmailBodyMain = "", EmailBody = "", EmailSubject = "";
                List<EmployeeConfig> empConfig = new List<EmployeeConfig>();
                List<string> empColumns = new List<string>();
                // Ranjit 18/02/2019 Smollan YTD Changes
                bool YTDFlag = false;
                string Month = Convert.ToString((int)myQueItem.month);
                string Year = Convert.ToString(myQueItem.year);
                InputMonths month = myQueItem.month;
                DateTime PayDate = new DateTime(int.Parse(Year), int.Parse(Month), 1);
                bool payHeadSett = false;
                List<PayrollSet> payDefaultSett = new List<PayrollSet>();
                string payFontSett = "";
                List<EmpTransCustomDetails> lstmodel = new List<EmpTransCustomDetails>();
                List<ComboValues> dataCombo = new List<ComboValues>();
                List<CustomeFieldDetail> customFieldData = new List<CustomeFieldDetail>();
                List<EmpTransCustomDetails> empTransCustomData = new List<EmpTransCustomDetails>();
                var wpCompanyName = "";
                List<WAMessageLog> wAMessage = new List<WAMessageLog>();
                List<PayslipFailedMailLog> failedMailLogs = new List<PayslipFailedMailLog>();
                List<LeaveAbsent> leaveAbsents = new List<LeaveAbsent>();
                List<LeaveCreditMonthly> LeaveCreditMonthly = new List<LeaveCreditMonthly>();
                List<SelfServiceDebitLeave> selfServiceDebitLeave = new List<SelfServiceDebitLeave>();
                string strEmployeeStatusFilter = string.Empty;
                int FilterConditionEmpStatus = 0;
                List<InputMonths> MonthList = new List<InputMonths>();
                List<int> yearList = new List<int>();
                FilterConditionEmpStatus = myQueItem.filterConditionEmpStatusPayslip;
                var smtpsetting = new SMTPSettings();
                var smtpsReceiversEmail = "";

                if (SubmitType == "Send Email")
                {
                    smtpsetting = _companyRepository.GetSMTPSettings(SchemaName, companyId);
                }

                if (FilterConditionEmpStatus == 1)
                {
                    strEmployeeStatusFilter = "Live";
                }
                else if (FilterConditionEmpStatus == 2)
                {
                    strEmployeeStatusFilter = "Left";
                }
                else
                {
                    strEmployeeStatusFilter = "All";
                }

                int intYear = int.Parse(Year);
                MemoryStream ms = new MemoryStream(new byte[0]);
                var emailData = new EmailTemplates();
                using (var ctx = new ApplicationDbContext().GetSchemaChangeDbContext(SchemaName))
                {
                    ctx.Database.SetCommandTimeout(0);

                    #region Payslip Progress
                    //if (SchemaName == "Smollfe37426078" || SchemaName == "MGMHEe93284675e" || SchemaName == "Orionb8f410a437" || SchemaName == "Total4cd8b47020")
                    if (PayslipsSchemas.Contains(SchemaName))
                    {
                        BackgroundPayrollProcess processObject = new BackgroundPayrollProcess();
                        processObject.schemaName = SchemaName;
                        processObject.UserId = userId;
                        processObject.Message = "Fetching Payslip Settings & Employee Details";
                        processObject.PercentCount = 0;
                        processObject.EmployeeCount = 0;
                        processObject.companyId = companyId;
                        SendPayslipNotifications(processObject, "payslip-progress");
                    }
                    #endregion
                    var dataComp = await ctx.Company.Where(c => c.Id == companyId).Select(c => c).FirstOrDefaultAsync();
                    wpCompanyName = dataComp.CompanyName;
                    if (SubmitType == "Send Email")
                    {
                        string msg = "";
                        //string msg = mailService.CheckSMTPSettings(SchemaName, companyId);
                        if (msg != "")
                        {
                            Msg = "message|" + msg;
                            return Msg;
                            //--------------------------- viewbag error ---------------------------- ViewBag.Message = msg;
                            //--------------------------- viewbag error ----------------------------  return View();
                        }
                        emailData = ctx.EmailTemplates.Where(e => e.CompanyId == companyId && e.TemplateName == "PaySlip").Select(e => e).FirstOrDefault();
                        EmailSubject = emailData.Subject;
                        EmailBodyMain = emailData.Body;
                        // Replace Special
                        EmailSubject = EmailSubject.Replace("[MONTH]", month.ToString());
                        EmailSubject = EmailSubject.Replace("[YEAR]", Year.ToString());
                        EmailBodyMain = EmailBodyMain.Replace("[COMPANYNAME|SPECIAL]", dataComp.CompanyName);
                        EmailBodyMain = EmailBodyMain.Replace("[COMPANYADDRESS|SPECIAL]", dataComp.Address);
                        EmailBodyMain = EmailBodyMain.Replace("[COMPANYCITY|SPECIAL]", dataComp.City);
                        EmailBodyMain = EmailBodyMain.Replace("[COMPANYSTATE|SPECIAL]", dataComp.State);
                        EmailBodyMain = EmailBodyMain.Replace("[COMPANYPINCODE|SPECIAL]", dataComp.PinCode);
                        EmailBodyMain = EmailBodyMain.Replace("[COMPANYTELEPHONE|SPECIAL]", dataComp.Telephone);
                        EmailBodyMain = EmailBodyMain.Replace("[MONTH]", month.ToString());
                        EmailBodyMain = EmailBodyMain.Replace("[YEAR]", Year.ToString());
                        EmailBody = EmailBody.Replace("[COMPANYNAME]", dataComp.CompanyName);
                        EmailBody = EmailBody.Replace("[ADDRESS]", dataComp.Address);
                        EmailBody = EmailBody.Replace("[CITY]", dataComp.City);
                        EmailBody = EmailBody.Replace("[STATE]", dataComp.State);
                        EmailBody = EmailBody.Replace("[PINCODE]", dataComp.PinCode);
                        EmailBody = EmailBody.Replace("[TELEPHONE]", dataComp.Telephone);
                        EmailBody = EmailBody.Replace("[BANKNAME]", dataComp.BankName);
                        EmailBody = EmailBody.Replace("[BANKADDRESS]", dataComp.BankAddress);
                        EmailBody = EmailBody.Replace("[GROUPCODE]", dataComp.GroupCode);
                        EmailBody = EmailBody.Replace("[EMPLOYERCODE]", dataComp.EmployerCode);
                        EmailBody = EmailBody.Replace("[REPORTDATE|SPECIAL]", DateTime.Now.Date.ToString("dd/MM/yyyy"));
                        EmailBody = EmailBody.Replace("[REPORTTIME|SPECIAL]", DateTime.Now.ToString("h:mm:ss tt"));
                    }
                    var dBManager = new DBConnectionManager(connectionString);
                    var document = new Document(PageSize.A4, 10, 10, 25, 25);
                    List<byte[]> memorylist = new List<byte[]>();
                    // Create a new PdfWrite object, writing the output to a MemoryStream
                    var output = new MemoryStream();
                    var writer = PdfWriter.GetInstance(document, output);
                    string conData = string.Empty;
                    //string PDF = Convert.ToString(form["PDF"]);
                    string WorkSheet = myQueItem.WorkSheet;
                    string Leave = myQueItem.Leave;
                    string Supress = myQueItem.Supress;
                    string categories = myQueItem.categories;
                    string multiEmp = string.Empty;
                    List<int> multiEmpList = myQueItem.multiEmpList;

                    int EmpId = myQueItem.EmpId;
                    List<int> selectedCategory = new List<int>();
                    foreach (var category in categories.Split(','))
                    {
                        selectedCategory.Add(int.Parse(category));
                    }
                    PdfPTable OuterTable = null;
                    PdfPTable MainTable = null;
                    PdfPTable EarningTable = null;
                    PdfPTable DeductionTable = null;
                    List<string> displayLeaves = new List<string>();
                    List<EmployeeDetail> empList = new List<EmployeeDetail>();
                    List<EmployeeDetail> empListNoEmail = new List<EmployeeDetail>();
                    List<LeaveOpening> empLeaveDetails = new List<LeaveOpening>();
                    List<LeaveMaster> lstLeaveMaster = new List<LeaveMaster>();
                    LeaveSet leaveSet = new LeaveSet();
                    payDefaultSett = await ctx.PayrollSet.Where(m => m.CompanyId == companyId && m.SetName == PayrollSetName.Payslip && m.SetType == "TEMPLATE").Select(m => m).ToListAsync();
                    if (payDefaultSett.Any(a => a.FieldValue2 == "DESIGNER") && SubmitType == "Download" && getPaySlipValidationSet(SchemaName, companyId).ToUpper() == "CUSTOM")
                    {
                        return "completed|DESIGNER";
                    }
                    if (payFontSett != "")
                    {
                        fntTableFont = FontFactory.GetFont(payFontSett, 8, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                        fntTableFontBold = FontFactory.GetFont(payFontSett, 8, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                        fntTableFontComp = FontFactory.GetFont(payFontSett, 9, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                        fntTableFontCompBold = FontFactory.GetFont(payFontSett, 9, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                    }
                    var fontData = ctx.PayrollSet.Where(p => p.CompanyId == companyId && p.SetName == PayrollSetName.Payslip && p.FieldValue1 == "COMPCODE" && p.FieldValue2 == "Font_Family").FirstOrDefault();
                    if (fontData != null)
                    {
                        payFontSett = fontData.FieldValue3;
                    }
                    empConfig = await ctx.EmployeeConfig.Where(e => e.CompanyId == companyId).Select(e => e).ToListAsync();
                    empColumns = empConfig.Select(a => a.FieldName).ToList();
                    dataCombo = await ctx.ComboValues.Where(m => m.CompanyId == companyId).ToListAsync();
                    recEmail = ctx.SMTPSettings.Where(s => s.CompanyId == companyId).Select(s => s.ReceiversEmail).FirstOrDefault();
                   


                    if (multiEmpList != null && multiEmpList.Count > 0)
                    {
                        if (stopPay)
                        {
                           
                            var query = "select EmployeeId, NetPay from " + SchemaName + ".EmpTransaction where CompanyId =" + companyId + " and ProcessDate = '" + PayDate.ToString("yyyy-MM-dd") + "' and (StopPayment = 1 OR StopPayment = 0) and EmployeeId in (" + string.Join(",", multiEmpList) + ")";
                            empTranscData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<EmpTransaction>(dBManager.GetDataTable(query, CommandType.Text));
                        }
                        else
                        {
                            var query = "select EmployeeId, NetPay from " + SchemaName + ".EmpTransaction where CompanyId =" + companyId + " and ProcessDate = '" + PayDate.ToString("yyyy-MM-dd") + "' and StopPayment = 0 and EmployeeId in (" + string.Join(",", multiEmpList) + ")";
                            empTranscData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<EmpTransaction>(dBManager.GetDataTable(query, CommandType.Text));
                        }
                    }
                    else
                    {
                        if (stopPay)
                        {
                            if (EmpId != 0)
                            {
                                
                                var query = "select EmployeeId, NetPay from " + SchemaName + ".EmpTransaction where CompanyId =" + companyId + " and ProcessDate = '" + PayDate.ToString("yyyy-MM-dd") + "' and StopPayment = 1 and EmployeeId =" + EmpId;
                                empTranscData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<EmpTransaction>(dBManager.GetDataTable(query, CommandType.Text));
                            }
                            else
                            {
                               
                                var query = "select EmployeeId, NetPay from " + SchemaName + ".EmpTransaction where CompanyId =" + companyId + " and ProcessDate = '" + PayDate.ToString("yyyy-MM-dd") + "' and (StopPayment = 1 OR StopPayment = 0)";
                                empTranscData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<EmpTransaction>(dBManager.GetDataTable(query, CommandType.Text));
                            }
                        }
                        else
                        {
                            if (EmpId != 0)
                            {
                                var query = "select EmployeeId, NetPay from " + SchemaName + ".EmpTransaction where CompanyId =" + companyId + " and ProcessDate = '" + PayDate.ToString("yyyy-MM-dd") + "' and StopPayment = 0 and EmployeeId =" + EmpId;
                                empTranscData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<EmpTransaction>(dBManager.GetDataTable(query, CommandType.Text));

                            }

                            else
                            {
                                
                                var query = "select EmployeeId, NetPay from " + SchemaName + ".EmpTransaction where CompanyId =" + companyId + " and ProcessDate = '" + PayDate.ToString("yyyy-MM-dd") + "' and StopPayment = 0";
                                empTranscData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<EmpTransaction>(dBManager.GetDataTable(query, CommandType.Text));
                            }

                        }
                    }
                    if (empTranscData.Count == 0)
                    {
                        return "error|Employees Transaction Data Not found";
                    }

                    List<int> transEmpId = empTranscData.Select(a => a.EmployeeId).ToList();
                    DateTime DateOfJoiningCheck = new DateTime(int.Parse(Year), int.Parse(Month), DateTime.DaysInMonth(int.Parse(Year), int.Parse(Month)));
                    if (SubmitType == "Send Email" && smtpsetting != null)
                    {
                        if (smtpsetting.ReceiversEmail == ReceiversEmail.EmailID)
                        {
                            smtpsReceiversEmail = "Email";
                        }
                        else
                        {
                            smtpsReceiversEmail = "PersonalEmail";
                        }
                    }
                    if (EmpId == 0)
                    {
                        if (multiEmpList.Count > 0)
                        {
                            if (Filter)
                            {
                                if (strEmployeeStatusFilter == "Live")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && multiEmpList.Contains(e.Id) && FilterIds.Contains(e.Id) && transEmpId.Contains(e.Id) && e.EmpStatus == EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else if (strEmployeeStatusFilter == "Left")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && multiEmpList.Contains(e.Id) && FilterIds.Contains(e.Id) && transEmpId.Contains(e.Id) && e.EmpStatus != EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && multiEmpList.Contains(e.Id) && FilterIds.Contains(e.Id) && transEmpId.Contains(e.Id)).Select(e => e).ToListAsync();
                                }
                            }
                            else
                            {
                                if (strEmployeeStatusFilter == "Live")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && multiEmpList.Contains(e.Id) && transEmpId.Contains(e.Id) && e.EmpStatus == EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else if (strEmployeeStatusFilter == "Left")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && multiEmpList.Contains(e.Id) && transEmpId.Contains(e.Id) && e.EmpStatus != EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && multiEmpList.Contains(e.Id) && transEmpId.Contains(e.Id)).Select(e => e).ToListAsync();
                                }
                            }
                        }
                        else
                        {
                            if (Filter)
                            {
                                if (strEmployeeStatusFilter == "Live")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && FilterIds.Contains(e.Id) && transEmpId.Contains(e.Id) && e.EmpStatus == EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else if (strEmployeeStatusFilter == "Left")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && FilterIds.Contains(e.Id) && transEmpId.Contains(e.Id) && e.EmpStatus != EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && FilterIds.Contains(e.Id) && transEmpId.Contains(e.Id)).Select(e => e).ToListAsync();
                                }
                            }
                            else
                            {
                                if (strEmployeeStatusFilter == "Live")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && transEmpId.Contains(e.Id) && e.EmpStatus == EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else if (strEmployeeStatusFilter == "Left")
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && transEmpId.Contains(e.Id) && e.EmpStatus != EmployeeStatus.Live).Select(e => e).ToListAsync();
                                }
                                else
                                {
                                    empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.DateOfJoining <= DateOfJoiningCheck && selectedCategory.Contains(e.Category) && transEmpId.Contains(e.Id)).Select(e => e).ToListAsync();
                                }
                            }
                        }
                    }
                    else
                    {
                        empList = await ctx.EmployeeDetail.Where(e => e.CompanyId == companyId && e.Id == EmpId && e.DateOfJoining <= DateOfJoiningCheck && transEmpId.Contains(e.Id)).Select(e => e).ToListAsync();
                    }
                    if (empList.Count == 0)
                    {
                        return "error|No Records Found";
                    }

                   

                    List<int> empIds = new List<int>();
                    empIds = empList.Select(a => a.Id).ToList();
                    var finstartDate = new DateTime();
                    #region Leave Details
                    if (!string.IsNullOrEmpty(Leave))
                    {
                        DateTime EntryToDate = new DateTime();
                        DateTime EntryFromDate = new DateTime();
                        bool Flag = false;
                        int FinNo = 0;

                        var FinNo_BasedonDate = ctx.LeaveFinSett.Where(a => a.CompanyId == companyId && a.FinStart <= PayDate && a.FinEnd >= PayDate).FirstOrDefault() ?? new LeaveFinSett();
                        var LeaveSet = ctx.LeaveSet.Where(m => m.CompanyId == companyId && m.FinNo == FinNo_BasedonDate.FinNo && (m.SetType == "FIN_START" || m.SetType == "FIN_END" || m.SetType == "LEVDATE")).ToList();

                        if (FinNo_BasedonDate.FinNo != 0)
                        {
                            foreach (var FinNos in LeaveSet.Select(m => m.FinNo).Distinct())
                            {
                                var Data = LeaveSet.Where(m => m.FinNo == FinNos).ToList();
                                DateTime FromDt = DateTime.ParseExact(Data.Where(m => m.SetType == "FIN_START").Select(m => m.FieldValue1).FirstOrDefault(), "dd/MM/yyyy", CultureInfo.InvariantCulture);
                                DateTime ToDt = DateTime.ParseExact(Data.Where(m => m.SetType == "FIN_END").Select(m => m.FieldValue1).FirstOrDefault(), "dd/MM/yyyy", CultureInfo.InvariantCulture);
                                finstartDate = FromDt;
                                if (FromDt <= PayDate && ToDt >= PayDate)
                                {
                                    if (Data.Count == 3)
                                    {
                                        EntryToDate = new DateTime(PayDate.Year, PayDate.Month, int.Parse(Data.Where(m => m.SetType == "LEVDATE").Select(m => m.FieldValue2).FirstOrDefault()));
                                        EntryFromDate = EntryToDate.AddMonths(-1);
                                        EntryFromDate = EntryFromDate.AddDays(1);
                                        Flag = true;
                                    }
                                    FinNo = FinNos;
                                }
                            }
                            if (!Flag)
                            {
                                EntryFromDate = PayDate;
                                EntryToDate = EntryFromDate.AddMonths(1);
                                EntryToDate = EntryToDate.AddDays(-1);
                            }
                            var empdetailsIds = empList.Select(a => a.Id).ToList();
                            //leaveAbsents = ctx.LeaveAbsent.Where(a => a.FinNo == FinNo_BasedonDate.FinNo && empdetailsIds.Contains(a.EmployeeId)).ToList();

                            var LeaveData = (from a in ctx.LeaveOpening
                                             join b in ctx.LeaveMaster on a.Leave equals b.Leave
                                             where a.CompanyId == companyId && b.CompanyId == companyId && a.FinNo == FinNo && b.FinNo == FinNo && b.ShowInPaySlip == true && empdetailsIds.Contains(a.EmployeeId)
                                             select new { a, b }).ToList();
                            //var LeaveData = (from
                            //                 //from b in ctx.LeaveAbsent
                            //                 //             join 
                            //                 a in ctx.LeaveOpening 
                            //    //on new { b1 = b.Leave, b2 = b.EmployeeId } equals new { b1 = a.Leave, b2 = a.EmployeeId }
                            //                 join c in ctx.LeaveMaster on a.Leave equals c.Leave
                            //                 where a.CompanyId == companyId && c.CompanyId == companyId &&
                            //                 a.FinNo == FinNo && c.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) && c.ShowInPaySlip == true
                            //                 //&& b.LeaveDate.Date >= finstartDate.Date && b.LeaveDate.Date <= EntryToDate.Date
                            //                 select new { a, c }).Distinct().ToList();

                            empLeaveDetails = LeaveData.Select(a => a.a).ToList();
                            leaveSet = ctx.LeaveSet.Where(m => m.CompanyId == companyId && m.FinNo == FinNo && m.SetName == SetName.CreditMonthly).FirstOrDefault();
                            LeaveMaster tempdata = new LeaveMaster();
                            if (LeaveData.Count != 0)
                            {
                                tempdata = LeaveData[0].b;
                                //tempdata = LeaveData[0].c;
                                // ram add
                                // lstLeaveMaster = LeaveData.Select(a => a.b).ToList();

                                var finstartDate1 = new DateTime();
                                finstartDate1 = finstartDate.Date;
                                while (finstartDate1.Date < EntryToDate.Date)
                                {
                                    if (finstartDate1.Date.Month == EntryToDate.Date.Month && EntryToDate.Date.Year == finstartDate1.Date.Year)
                                    {
                                        MonthList.Add((InputMonths)finstartDate1.Date.Month);
                                        break;
                                    }
                                    finstartDate1 = finstartDate1.Date.AddMonths(1).AddDays(-1);
                                    MonthList.Add((InputMonths)finstartDate1.Date.Month);
                                }

                                if (finstartDate.Date.Year != EntryToDate.Date.Year)
                                {
                                    yearList.Add(finstartDate.Date.Year);
                                    yearList.Add(EntryToDate.Date.Year);
                                }
                                else
                                {
                                    yearList.Add(EntryToDate.Date.Year);
                                }
                                lstLeaveMaster = ctx.LeaveMaster.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && a.ShowInPaySlip == true).ToList();
                                var leaveList = lstLeaveMaster.Select(a => a.Leave).Distinct().ToList();

                                //leaveAbsents = ctx.LeaveAbsent.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) && leaveList.Contains(a.Leave)
                                //&& a.LeaveDate.Date >= finstartDate.Date && a.LeaveDate.Date <= EntryToDate.Date).ToList();

                                leaveAbsents = ctx.LeaveAbsent.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) && leaveList.Contains(a.Leave)
                                && MonthList.Contains((InputMonths)a.LeaveDate.Date.Month) && yearList.Contains(a.LeaveDate.Date.Year)).ToList();

                                LeaveCreditMonthly = ctx.LeaveCreditMonthly.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) && leaveList.Contains(a.Leave)
                                && MonthList.Contains(a.InputMonth) && yearList.Contains(a.InputYear)
                                ).ToList();

                                selfServiceDebitLeave = ctx.SelfServiceDebitLeave.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) &&
                                MonthList.Contains((InputMonths)a.LeaveDate.Month) && yearList.Contains(a.LeaveDate.Year)).ToList();
                            }
                            else
                            {
                                lstLeaveMaster = ctx.LeaveMaster.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && a.ShowInPaySlip == true).ToList();
                                tempdata = lstLeaveMaster.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && a.ShowInPaySlip == true).FirstOrDefault();

                                var leaveList = lstLeaveMaster.Select(a => a.Leave).Distinct().ToList();
                                leaveAbsents = ctx.LeaveAbsent.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) && leaveList.Contains(a.Leave)
                                && a.LeaveDate.Date >= finstartDate.Date && a.LeaveDate.Date <= EntryToDate.Date).ToList();

                                while (finstartDate.Date < EntryToDate.Date)
                                {
                                    if (finstartDate.Date.Month <= EntryToDate.Date.Month && EntryToDate.Date.Year == finstartDate.Date.Year)
                                    {
                                        break;
                                    }
                                    finstartDate = finstartDate.Date.AddMonths(1).AddDays(-1);
                                    MonthList.Add((InputMonths)finstartDate.Date.Month);
                                }

                                if (finstartDate.Date.Year != EntryToDate.Date.Year)
                                {
                                    yearList.Add(finstartDate.Date.Year);
                                    yearList.Add(EntryToDate.Date.Year);
                                }
                                else
                                {
                                    yearList.Add(EntryToDate.Date.Year);
                                }
                                LeaveCreditMonthly = ctx.LeaveCreditMonthly.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) && leaveList.Contains(a.Leave)
                                && MonthList.Contains(a.InputMonth) && yearList.Contains(a.InputYear)).ToList();

                                selfServiceDebitLeave = ctx.SelfServiceDebitLeave.Where(a => a.CompanyId == companyId && a.FinNo == FinNo && empdetailsIds.Contains(a.EmployeeId) &&
                                MonthList.Contains((InputMonths)a.LeaveDate.Month) && yearList.Contains(a.LeaveDate.Year)).ToList();
                            }
                            if (tempdata != null)
                            {
                                if (tempdata.DisplayBalance == true)
                                {
                                    displayLeaves.Add("balance");
                                }
                                if (tempdata.DisplayCredit == true)
                                {
                                    displayLeaves.Add("credit");
                                }
                                if (tempdata.DisplayOpening == true)
                                {
                                    displayLeaves.Add("opening");
                                }
                                if (tempdata.DisplayUsed == true)
                                {
                                    displayLeaves.Add("used");
                                }
                                if (tempdata.DisplayDebit == true)
                                {
                                    displayLeaves.Add("debit");
                                }
                            }

                        }
                    }
                    #endregion
                    List<CategoryMaster> lstPayConfigNo = await ctx.CategoryMaster.Where(c => c.CompanyId == companyId).Select(c => c).ToListAsync();
                    List<PaySlipPrint> NoOrderNumber = await ctx.PaySlipPrint.Where(c => c.CompanyId == companyId).Select(c => c).ToListAsync();
                    var PrintYesFields = await ctx.AllConfig.Where(c => c.CompanyId == companyId && c.IncludeGross == true).Select(c => c).ToListAsync();
                    var payrollReprotData = await ctx.ReportMetaData.Where(a => (a.ViewName == "Payslip2" || a.ViewName == "PaySlip" || a.ViewName == "PaySlip3" || a.ViewName == "PaySlip4") && a.CompanyId == companyId).Select(a => a).ToListAsync();

                    ReportMetaData vData2 = new ReportMetaData();
                    ReportMetaData vData = new ReportMetaData();

                    //if (SchemaName == "Smollfe37426078" || SchemaName == "MGMHEe93284675e" || SchemaName == "Orionb8f410a437" || SchemaName == "Total4cd8b47020")
                    if (PayslipsSchemas.Contains(SchemaName))
                    {
                        vData2 = payrollReprotData.Where(a => a.ViewName == "Payslip3" && a.CompanyId == companyId).Select(a => a).FirstOrDefault();
                        vData = payrollReprotData.Where(a => a.ViewName == "PaySlip4" && a.CompanyId == companyId).Select(a => a).FirstOrDefault();
                    }
                    else
                    {
                        vData2 = payrollReprotData.Where(a => a.ViewName == "Payslip2" && a.CompanyId == companyId).Select(a => a).FirstOrDefault();
                        vData = payrollReprotData.Where(a => a.ViewName == "PaySlip" && a.CompanyId == companyId).Select(a => a).FirstOrDefault();

                    }

                    var lstdataOtherSett = await ctx.PaySlipOtherSetting.Where(t => t.CompanyId == companyId).Select(a => a).ToListAsync();
                    var lstHeaderFields = await ctx.PaySlipSetting.Where(p => p.CompanyId == companyId).OrderBy(p => p.HeaderPrintOrder).ToListAsync();
                    var fieldNames = lstHeaderFields.Where(a => a.ComponentType == ComponentType.ManualFields).Select(a => a.FieldName).ToList();
                    var customData = await ctx.CustomeFieldDetail.Where(a => a.CompanyId == companyId && fieldNames.Contains(a.CustomeFieldName) && empIds.Contains(a.EmployeeId)).ToListAsync();
                    List<CustomeFieldDetail> customDataEmp = new List<CustomeFieldDetail>();
                    if (payDefaultSett.Any())
                        customDataEmp = await ctx.CustomeFieldDetail.Where(c => c.CompanyId == companyId && empIds.Contains(c.EmployeeId)).Select(c => c).ToListAsync();
                    var listLoanMaster = await ctx.LoanMaster.Where(m => m.CompanyId == companyId).ToListAsync();
                    var LoanInstallment = await ctx.MonthLoan.Where(a => a.CompanyId == companyId && a.MonthlyDate == PayDate).ToListAsync();
                    InputMonths enumMonth = month;
                    string year = myQueItem.year.ToString();
                    string strYear = year.ToString();
                    var TransactionData = await ctx.EmpTransCustomDetails.Where(m => m.CompanyId == companyId && m.Month == enumMonth && m.Year == strYear && fieldNames.Contains(m.FieldName) && empIds.Contains(m.EmployeeId)).Select(m => m).ToListAsync();
                    var leaveFinAll = await ctx.LeaveFinSett.Where(a => a.CompanyId == companyId).ToListAsync();
                    var leaveFinNo = leaveFinAll.Where(a => a.CompanyId == companyId && a.DefaultFin == true).Select(a => a.FinNo).FirstOrDefault();

                    #region Benefit Component
                    bool summary = false;
                    var dtBenComp = new DataTable();
                    List<BenComponent> lstTempBenCompData = new List<BenComponent>();
                    if (myQueItem.chkBenefitSlip != null && myQueItem.chkBenefitSlip != "")
                    {
                        summary = true;
                    }
                    else
                    {
                        summary = false;
                    }
                    if (summary == true)
                    {
                        var CommandText = "exec('Select A.CategoryId,A.CompanyId,A.FieldName,B.ReimbursementType,B.LabelName,A.FinNo  from " + SchemaName + ".BenComponent A," + SchemaName + ".EarningConfig B  where A.FieldType = ''I'' and   A.FieldName = B.FieldName and   A.CompanyId = B.CompanyId and  A.CompanyId = " + companyId + " and   A.FinNo in (select FinNo from " + SchemaName + ".BenFinSetting where CompanyId = " + companyId + " and DefaultFinNo = 1)   order by A.OrderNo')";
                        //var dtBenComp = new DataTable();
                        dtBenComp = dBManager.GetDataTable(CommandText, CommandType.Text);
                        //List<BenComponent> lstTempBenCompData = new List<BenComponent>();
                        foreach (DataRow item in dtBenComp.Rows)
                        {
                            lstTempBenCompData.Add(new BenComponent
                            {
                                CompanyId = int.Parse(item["CompanyId"].ToString()),
                                CategoryId = int.Parse(item["CategoryId"].ToString()),
                                FieldName = item["FieldName"].ToString(),
                                ReimbursementType = (ReimbursementType)Enum.Parse(typeof(ReimbursementType), Convert.ToString(item["ReimbursementType"])),
                                LabelName = item["LabelName"].ToString(),
                                FinNo = int.Parse(item["FinNo"].ToString())
                            });
                        }
                    }
                    else
                    {
                    }
                    #endregion
                    List<EarningConfig> earngColumns = new List<EarningConfig>();
                    List<DeductionConfig> dedColumns = new List<DeductionConfig>();
                    var earningsData = await ctx.EarningConfig.Where(a => a.CompanyId == companyId && a.FieldName == "PD").Select(a => a).ToListAsync();
                    var earndata = await (from a in ctx.EarningConfig
                                          join b in ctx.AllConfig on a.FieldName equals b.AdditionField
                                          where b.ComponentType == ComponentType.Earning && a.CompanyId == companyId
                                          select a).ToListAsync();
                    earningsData.AddRange(earndata);
                    earngColumns = earningsData.Distinct().ToList();
                    var deductionData = await (from a in ctx.DeductionConfig
                                               join b in ctx.AllConfig on a.FieldName equals b.AdditionField
                                               where b.ComponentType == ComponentType.Deduction && a.CompanyId == companyId
                                               select a).ToListAsync();
                    // b.Dirper == Operations.Direct  && a.Loan == false
                    dedColumns.AddRange(deductionData);
                    dedColumns = deductionData.Distinct().ToList();
                    bool flagPerPage = false;
                    int catId = 0, catPayConfigNo = 0, paySlipPerPage = 0, count = 0;
                    if (selectedCategory.Count != 0)
                    {
                        catId = selectedCategory[0];
                        catPayConfigNo = lstPayConfigNo.Where(c => c.CompanyId == companyId && c.Id == catId).Select(c => c.PayConfigNo).FirstOrDefault();

                    }
                    if (selectedCategory.Count != 0 && empList.Count > 1 && SubmitType != "Send Email")
                    {
                        paySlipPerPage = lstdataOtherSett.Where(t => t.CompanyId == companyId && t.PayConNo == catPayConfigNo).Select(t => t.PaySlipPerPage).FirstOrDefault();
                        count = 0;
                        if (catPayConfigNo != 0 && paySlipPerPage != 0 && paySlipPerPage > 0)
                        {
                            flagPerPage = true;
                        }
                        else
                        {
                            flagPerPage = false;
                        }
                    }
                    else
                    {
                        flagPerPage = false;
                    }
                    if (!string.IsNullOrEmpty(WorkSheet))
                    {
                        flagPerPage = false;
                    }
                    var payData2 = new DataTable();
                    var payData1 = new DataTable();
                    //if (SchemaName == "Smollfe37426078" || SchemaName == "MGMHEe93284675e" || SchemaName == "Orionb8f410a437" || SchemaName == "Total4cd8b47020")
                    if (PayslipsSchemas.Contains(SchemaName))
                    {


                        List<IEnumerable<int>> listOfLists = new List<IEnumerable<int>>();

                        for (int i = 0; i < empIds.Count(); i += 500)
                        {
                            listOfLists.Add(empIds.Skip(i).Take(500));
                        }
                        int listcount = 1;
                       

                        var paySlipprintQuery = "select FieldName, LabelName, ComponentType,CASE WHEN ComponentType = 2 THEN '0' WHEN ComponentType = 1 THEN '1' END AS ChequeType,PrintOrder from " + SchemaName + ".PaySlipPrint where PayConNo = " + catPayConfigNo.ToString() + " and CompanyId = " + companyId.ToString() + " and (ComponentType = 2 or ComponentType = 1) order by PrintOrder";
                        var payslipPrintData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<PaySlipPrint>(dBManager.GetDataTable(paySlipprintQuery, CommandType.Text));
                        var FieldNames = payslipPrintData.Select(a => "[" + a.FieldName + "]").Distinct().ToList();
                        var catIds = string.Join(",", selectedCategory);

                        // matching 

                        var paymatchingQuery = "select actualField, ComputeField from " + SchemaName + ".PaySlipMatching where companyid =" + companyId.ToString() + " and PayConNo = " + catPayConfigNo.ToString();
                        var paymatchingData = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<PaySlipMatching>(dBManager.GetDataTable(paymatchingQuery, CommandType.Text));
                        var computeFields = paymatchingData.Select(a => a.ComputeField).Distinct().ToList();
                        FieldNames.AddRange(computeFields);

                        FieldNames = FieldNames.Distinct().ToList();
                        var strFieldName = "";
                        var strFieldNameFor = "";
                        if (FieldNames.Count > 0)
                        {
                            strFieldName = ", " + string.Join(",", FieldNames);
                            strFieldNameFor = string.Join(",", FieldNames);
                        }

                        var emplst = myQueItem.multiEmpList;

                        var processDate = new DateTime(myQueItem.year, (int)myQueItem.month, 01).ToString("dd/MM/yyyy");
                        var unpivotQuery = "SELECT EmployeeId as Id, FieldName, Amount FROM ( SELECT EmployeeId" + strFieldName + " FROM " + SchemaName + ".view_" + SchemaName + "_" + companyId + " WHERE CompanyId = " + companyId + " [Emplst] AND categoryId IN (" + catIds + ") AND ProcessDate = CONVERT(DATETIME, '" + processDate + "', 103) ) AS Source UNPIVOT ( Amount FOR FieldName IN (" + strFieldNameFor + ") ) AS UnpivotedData";
                        if (emplst.Count > 0)
                        {
                            unpivotQuery = unpivotQuery.Replace("[Emplst]", " AND EmployeeId in (" + string.Join(",", emplst) + ")");
                        }
                        else
                        {
                            unpivotQuery = unpivotQuery.Replace("[Emplst]", "");
                        }
                        var unpivotData = dBManager.GetDataTable(unpivotQuery, CommandType.Text);

                        DataColumn newColumn = new DataColumn("Name", typeof(string));
                        unpivotData.Columns.Add(newColumn);
                        newColumn.SetOrdinal(0);
                        newColumn = new DataColumn("Code", typeof(string));
                        unpivotData.Columns.Add(newColumn);
                        newColumn.SetOrdinal(1);
                        newColumn = new DataColumn("LabelName", typeof(string));
                        unpivotData.Columns.Add(newColumn);
                        newColumn.SetOrdinal(5);
                        newColumn = new DataColumn("Match", typeof(string));
                        unpivotData.Columns.Add(newColumn);
                        newColumn = new DataColumn("PrintOrder", typeof(int));
                        unpivotData.Columns.Add(newColumn);
                        newColumn = new DataColumn("Flag", typeof(string));
                        unpivotData.Columns.Add(newColumn);
                        DataView filteredView = new DataView(unpivotData);
                       
                        var empDetailsLookup = empList.ToDictionary(a => a.Id, a => new { a.Code, a.FName, a.LName });
                        var paymatchingDataLookup = paymatchingData.ToDictionary(a => a.ActualField, a => a.ComputeField);
                        List<DataRow> rowsToDelete = new List<DataRow>(); // Track rows to delete

                        foreach (DataRow row in unpivotData.Rows)
                        {
                            int id = Convert.ToInt32(row["Id"]);

                            if (!empDetailsLookup.TryGetValue(id, out var empDetails))
                                continue;

                            var Lname = string.IsNullOrEmpty(empDetails.LName) ? "" : empDetails.LName;
                            row["Name"] = empDetails.FName + " " + Lname;
                            row["Code"] = empDetails.Code;

                            var fieldName = row["FieldName"].ToString();
                            var labelName = payslipPrintData.FirstOrDefault(a => a.FieldName == fieldName)?.LabelName;
                            row["LabelName"] = labelName;
                            if (!payslipPrintData.Any(a => a.FieldName == fieldName))
                            {
                                rowsToDelete.Add(row); // Add row to delete list
                                continue;
                            }
                            if (paymatchingDataLookup.TryGetValue(fieldName, out var computeField))
                            {
                                if (computeField != null)
                                {
                                    var amount = unpivotData.AsEnumerable()
                                        .Where(a => a.Field<int>("Id") == id && a.Field<string>("FieldName") == computeField)
                                        .Select(a => a.Field<decimal>("Amount"))
                                        .FirstOrDefault();

                                    row["Match"] = amount != 0 ? amount : 0;
                                }
                                else
                                {
                                    row["Match"] = 0;
                                }
                            }
                            else
                            {
                                row["Match"] = 0;
                            }
                            row["PrintOrder"] = payslipPrintData.FirstOrDefault(a => a.FieldName == fieldName)?.PrintOrder ?? 0;
                            row["Flag"] = payslipPrintData.FirstOrDefault(a => a.FieldName == fieldName)?.ChequeType ?? "0";
                        }
                        // Delete rows outside the loop
                        foreach (var rowToDelete in rowsToDelete)
                        {
                            rowToDelete.Delete();
                        }

                        // Accept changes to apply row deletions
                        unpivotData.AcceptChanges();

                        payData1 = unpivotData;

                        var payslipSettingData = lstHeaderFields.Where(a => a.ComponentType == ComponentType.Earning || a.ComponentType == ComponentType.Deduction).Select(a => new { a.FieldName, a.LabelName, a.HeaderPrintOrder, a.FooterPrintOrder, a.FFPrintOrder, a.ComponentType }).ToList();
                        var paySettFieldNames = payslipSettingData.Select(a => "[" + a.FieldName + "]").Distinct().ToList();
                        //payData1 = dBManager.GetDataTable(PayData, CommandType.Text);

                        var strpaySettFieldName = "";
                        var strpaySettFieldNameFor = "";
                        if (FieldNames.Count > 0)
                        {
                            strpaySettFieldName = ", " + string.Join(",", paySettFieldNames);
                            strpaySettFieldNameFor = string.Join(",", paySettFieldNames);
                        }

                        var unpivotQuery1 = "SELECT EmployeeId as Id, FieldName, Amount FROM ( SELECT EmployeeId" + strpaySettFieldName + " FROM " + SchemaName + ".view_" + SchemaName + "_" + companyId + " WHERE CompanyId = " + companyId + " [Emplst] AND categoryId IN (" + catIds + ") AND ProcessDate = CONVERT(DATETIME, '" + processDate + "', 103) ) AS Source UNPIVOT ( Amount FOR FieldName IN (" + strpaySettFieldNameFor + ") ) AS UnpivotedData";

                        if (emplst.Count > 0)
                        {
                            unpivotQuery1 = unpivotQuery1.Replace("[Emplst]", " AND EmployeeId in (" + string.Join(",", emplst) + ")");
                        }
                        else
                        {
                            unpivotQuery1 = unpivotQuery1.Replace("[Emplst]", "");
                        }
                        var unpivotData1 = dBManager.GetDataTable(unpivotQuery1, CommandType.Text);
                        DataColumn newColumn1 = new DataColumn("Name", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        newColumn1.SetOrdinal(0);
                        newColumn1 = new DataColumn("Code", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        newColumn1.SetOrdinal(1);
                        newColumn1 = new DataColumn("LabelName", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        newColumn1.SetOrdinal(5);
                        newColumn1 = new DataColumn("Match", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        newColumn1 = new DataColumn("HeaderPrintOrder", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        newColumn1 = new DataColumn("FooterPrintOrder", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        newColumn1 = new DataColumn("FFPrintOrder", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        newColumn1 = new DataColumn("Flag", typeof(string));
                        unpivotData1.Columns.Add(newColumn1);
                        foreach (DataRow row in unpivotData1.Rows)
                        {
                            int id = Convert.ToInt32(row["Id"]);
                            var empDetails = empList.Where(a => a.Id == id).Select(a => new { a.Code, a.FName, a.LName }).FirstOrDefault();
                            if (empDetails == null)
                            {
                                continue;
                            }
                            var Lname = string.IsNullOrEmpty(empDetails.LName) ? "" : empDetails.LName;
                            row["Name"] = empDetails.FName + " " + Lname;
                            row["Code"] = empDetails.Code;
                            row["LabelName"] = payslipSettingData.Where(a => a.FieldName == row["FieldName"].ToString()).Select(a => a.LabelName).FirstOrDefault();
                            row["Match"] = 0;
                            row["HeaderPrintOrder"] = lstHeaderFields.Where(a => a.FieldName == row["FieldName"].ToString()).Select(a => a.HeaderPrintOrder).FirstOrDefault();
                            row["FooterPrintOrder"] = lstHeaderFields.Where(a => a.FieldName == row["FieldName"].ToString()).Select(a => a.FooterPrintOrder).FirstOrDefault();
                            row["FFPrintOrder"] = lstHeaderFields.Where(a => a.FieldName == row["FieldName"].ToString()).Select(a => a.FFPrintOrder).FirstOrDefault();
                            row["Flag"] = payslipPrintData.Where(a => a.FieldName == row["FieldName"].ToString()).Select(a => a.ChequeType).FirstOrDefault(); ;
                        }
                        payData2 = unpivotData1;
                        var query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,'payData1 " + payData1.Rows.Count + " ')";
                        dBManager.Insert(query1, System.Data.CommandType.Text);
                        

                        #region Payslip Progress
                        //if (SchemaName == "Smollfe37426078" || SchemaName == "MGMHEe93284675e" || SchemaName == "Orionb8f410a437" || SchemaName == "Total4cd8b47020")
                        if (PayslipsSchemas.Contains(SchemaName))
                        {
                            BackgroundPayrollProcess processObject = new BackgroundPayrollProcess();
                            processObject.schemaName = SchemaName;
                            processObject.UserId = userId;
                            processObject.Message = "Fetching Income Tax Details of " + empIds.Count() + " Employees";
                            processObject.PercentCount = 0;
                            processObject.EmployeeCount = 0;
                            processObject.companyId = companyId;
                            SendPayslipNotifications(processObject, "payslip-progress");
                        }
                        #endregion
                       
                        query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,'payData2 " + payData2.Rows.Count + " ')";
                        dBManager.Insert(query1, System.Data.CommandType.Text);

                    }

                    string path = "";
                    bool isLogoExists = false;
                    string companyPath = "";
                    string cs = new StaticConfigs().GetServerConnection(ConfigurationType.BlobURL);
                    path = cs + "/companylogo-" + SchemaName.ToLower() + "/";
                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(new StaticConfigs().GetServerConnection(ConfigurationType.BlobConnectionString));// DevelopmentStorageAccount;
                    CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                    CloudBlobContainer container = blobClient.GetContainerReference("companylogo" + "-" + SchemaName.ToLower());
                    if (container.Exists())
                    {
                        CloudBlockBlob blockBlob = container.GetBlockBlobReference(SchemaName + "-" + companyId);
                        if (blockBlob.Exists())
                        {
                            isLogoExists = true;
                            path += SchemaName + "-" + companyId;

                            bool isexist = System.IO.Directory.Exists(payslipPath + "//CompanyLogo//" + userId);
                            if (!isexist)
                            {
                                System.IO.Directory.CreateDirectory(payslipPath + "//CompanyLogo//" + userId);
                            }

                            blockBlob.DownloadToFile(payslipPath + "//CompanyLogo//" + userId + "//" + blockBlob.Name, FileMode.Create);
                            companyPath = payslipPath + "//CompanyLogo//" + userId + "//" + blockBlob.Name;
                            // System.IO.File.WriteAllBytes(payslipPath.Replace('\\', '/') + "/" + userId + "/Payslips/" + emp.Code.Replace('/', '_').ToString() + ".pdf", output1.ToArray());

                            //return Json(new { data = path, flag = true }, JsonRequestBehavior.AllowGet);
                        }
                    }
                    //*** Cumulative Option in Payslip ***//

                    var transactionDataLst = new List<EmpTransaction>();
                    if (empList.Count > 0)
                    {
                        int PayConfigNo = lstPayConfigNo.Where(c => c.CompanyId == companyId && c.Id == empList[0].Category).Select(c => c.PayConfigNo).FirstOrDefault();

                        var dataOtherSett = lstdataOtherSett.Where(t => t.CompanyId == companyId && t.PayConNo == PayConfigNo).FirstOrDefault();
                        if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                        {
                            lstmodel = new List<EmpTransCustomDetails>();
                            DateTime startDate = PayDate;
                            if ((int)dataOtherSett.CummMonth > PayDate.Month)
                            {
                                startDate = new DateTime(PayDate.Year - 1, (int)dataOtherSett.CummMonth, 1);
                            }
                            else
                            {
                                startDate = new DateTime(PayDate.Year, (int)dataOtherSett.CummMonth, 1);
                            }
                            var transEmpIds = empList.Select(a => a.Id).ToList();
                            transactionDataLst = ctx.EmpTransaction.Where(a => a.CompanyId == companyId && transEmpIds.Contains(a.EmployeeId) && a.ProcessDate >= startDate && a.ProcessDate <= PayDate).Select(a => a).ToList();
                        }
                        if (SubmitType == "Send Email")
                        {
                            var transEmpIds = empList.Where(e => e.CompanyId == companyId && (string.IsNullOrWhiteSpace(smtpsReceiversEmail) || (smtpsReceiversEmail == "Email" ? !string.IsNullOrEmpty(e.Email) : smtpsReceiversEmail == "PersonalEmail" ? !string.IsNullOrEmpty(e.PersonalEmail) : false))).Select(a => a.Id).ToList();
                            //customFieldData = ctx.CustomeFieldDetail.Where(m => m.CompanyId == companyId && transEmpIds.Contains(m.EmployeeId)).ToList();
                            empTransCustomData = ctx.EmpTransCustomDetails.Where(m => m.CompanyId == companyId && transEmpIds.Contains(m.EmployeeId) && m.Month == month && m.Year == year).ToList();
                        }
                    }
                    int Percentagecount = 0;

                    #region Worksheet
                    var companyDetail = new Company();
                    List<FinancialSett> lstfinSetData = new List<FinancialSett>();
                    List<TaxRepSet> lstTaxRepset = new List<TaxRepSet>();
                    List<TaxConfig> taxConfigData = new List<TaxConfig>();
                    List<TaxIncomeMatch> taxIncomeMatchData = new List<TaxIncomeMatch>();
                    List<EarningConfig> lstEarnigC = new List<EarningConfig>();
                    List<TaxPreIncome> lstTaxPreIncome = new List<TaxPreIncome>();
                    List<TaxSlab> lstTaxSlab = new List<TaxSlab>();
                    List<TaxIncome> lstTaxIncome = new List<TaxIncome>();
                    List<TaxCalculation> lstTaxCalculation = new List<TaxCalculation>();
                    DataTable data2_worksheet = new DataTable();
                    DataTable data3_worksheet = new DataTable();
                    DataTable data_worksheet = new DataTable();
                    List<TaxPreIncome> previousEmployerTDS = new List<TaxPreIncome>();
                    List<EmpTransCustomDetails> lstEmpTransCustomDetails = new List<EmpTransCustomDetails>();
                    List<TaxSlab> taxSlabData = new List<TaxSlab>();
                    List<TaxRepSet> lstTaxRepSet = new List<TaxRepSet>();
                    List<TaxIncome> TaxIncomeQuery = new List<TaxIncome>();
                    List<FinancialSett> finSetData = new List<FinancialSett>();
                    if (!string.IsNullOrEmpty(WorkSheet))
                    {
                        DateTime ProcessDate_Worksheet = new DateTime(int.Parse(year), (int)month, 1);

                        companyDetail = ctx.Company.Where(c => c.Id == companyId).FirstOrDefault();
                        finSetData = ctx.FinancialSett.Where(a => a.CompanyId == companyId && a.StartDate <= ProcessDate_Worksheet && a.EndDate >= ProcessDate_Worksheet).ToList();
                        var Financialnumber = finSetData.Where(a => a.CompanyId == companyId && a.StartDate <= ProcessDate_Worksheet && a.EndDate >= ProcessDate_Worksheet).Select(a => a.FinNo).FirstOrDefault();
                        lstTaxRepSet = ctx.TaxRepSet.Where(c => c.FinNo == Financialnumber).Select(c => c).ToList();
                        taxConfigData = ctx.TaxConfig.Where(a => a.FinNo == Financialnumber).ToList();
                        taxSlabData = ctx.TaxSlab.Where(t => t.FinNo == Financialnumber).ToList();
                        taxIncomeMatchData = ctx.TaxIncomeMatch.Where(a => a.FinNo == Financialnumber).ToList();
                        previousEmployerTDS = ctx.TaxPreIncome.Where(s => s.FinNo == Financialnumber).ToList();
                        lstEarnigC = ctx.EarningConfig.Where(a => a.CompanyId == companyId).ToList();
                        TaxIncomeQuery = ctx.TaxIncome.Where(c => c.CompanyId == companyId && c.ProcessDate == ProcessDate_Worksheet).ToList();
                        var TaxcalculationQuery = " select * from " + SchemaName + ".taxcalculation  where finno=" + Financialnumber + " and CompanyId =" + companyId + "  and processdate= convert(datetime,'" + ProcessDate_Worksheet.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "',103) ";
                        lstTaxCalculation = new PocketHRMS.DAL.Repositories.DataTableExtensions().ConvertDataTable<TaxCalculation>(dBManager.GetDataTable(TaxcalculationQuery, CommandType.Text));

                        #region Initialize_Calculation

                        //var cmd = new Greytrix.Pocket.Models.ApplicationDbContext(schemaName).Database.Connection.CreateCommand();
                        var vData1 = ctx.ReportMetaData.Where(a => a.CompanyId == companyId && a.ViewName == "WorkSheet").Select(a => a).FirstOrDefault();

                        string where = " where A.companyId = " + companyId.ToString() + " and A.FinNo = " + Financialnumber.ToString() + " and A.ProcessDate = convert(datetime,''" + ProcessDate_Worksheet.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103)";
                        if (EmpId != 0)
                        {
                            where += " and A.EmployeeId = " + EmpId.ToString();
                        }
                        conData = vData1.Query.Replace("dbo", SchemaName);
                        conData = conData.Replace("@condition", where);

                        //var dBManager = new DBConnectionManager(connectionString);
                        data_worksheet = dBManager.GetDataTable(conData, CommandType.Text);



                        #endregion

                        //string Query = "exec('select A.TDS,A.EmployeeId from dbo.EmpTransaction A join dbo.TaxCalculation B on A.companyId = B.companyId and A.EmployeeId = B.EmployeeId and A.ProcessDate = B.ProcessDate where  B.LastProcess = 1 and A.ProcessDate = convert(datetime,''" + ProcessDate.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103)')";
                        string Query = "exec('select A.TDS,A.EmployeeId from dbo.EmpTransaction A join dbo.TaxCalculation B on A.companyId = B.companyId and A.EmployeeId = B.EmployeeId and A.ProcessDate = B.ProcessDate where  B.LastProcess = 1 and A.CompanyId =" + companyId + " and B.Companyid=" + companyId + "  and A.ProcessDate = convert(datetime,''" + ProcessDate_Worksheet.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103) and B.ProcessDate = convert(datetime,''" + ProcessDate_Worksheet.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103) [EMPIDCOND]')";

                        if (EmpId != 0)
                        {
                            Query = Query.Replace("[EMPIDCOND]", " and A.Employeeid=" + EmpId);
                        }
                        else
                        {
                            var empids = String.Join(",", empList.Select(a => a.Id).ToList());
                            Query = Query.Replace("[EMPIDCOND]", " and A.Employeeid in (" + empids + ")");
                        }
                        conData = Query.Replace("dbo", SchemaName);
                        data2_worksheet = dBManager.GetDataTable(conData, CommandType.Text);
                        bool WSheet_Annexure = (ctx.TaxRepSet.Where(m => m.EType == RepSet.WSheet_A && m.FinNo == Financialnumber).Count() > 0);
                        if (WSheet_Annexure && EmpId != 0)
                        {

                            List<string> FormulaFields = new List<string>();
                            string FormulaField1 = string.Empty;
                            string FormulaField2 = string.Empty;
                            string FormulaField3 = string.Empty;
                            List<TaxIncomeMatch> incomeMatchingList = new List<TaxIncomeMatch>();
                            LoadIncomeMatching(ref incomeMatchingList, SchemaName, Financialnumber, companyId, taxConfigData, taxIncomeMatchData, lstEarnigC);
                            List<string> deductioncomponents = new List<string>();
                            deductioncomponents.Add("PTAX");
                            deductioncomponents.Add("PFAMOUNT");
                            deductioncomponents.Add("TDS");
                            deductioncomponents.Add("ESI");

                            foreach (var item in incomeMatchingList)
                            {
                                if (!string.IsNullOrEmpty(item.Formula))
                                {
                                    FormulaField3 += item.FieldName + "=(t." + item.FieldName + "+t." + item.Formula.Replace("+", "+t.").Replace("-", "-t.") + "),";

                                    if (!FormulaFields.Contains(item.FieldName.Trim()))
                                    {
                                        FormulaFields.Add(item.FieldName.Trim());
                                    }

                                    string formula = item.Formula.Replace('+', ',').Replace('-', ',');
                                    foreach (var field in formula.Split(','))
                                    {
                                        if (!FormulaFields.Contains(field.Trim()))
                                        {
                                            if (!string.IsNullOrEmpty(field.Trim()))
                                            {
                                                FormulaFields.Add(field.Trim());
                                            }
                                        }
                                    }
                                }
                                else
                                {
                                    FormulaField3 += "t." + item.FieldName + ",";

                                    if (!FormulaFields.Contains(item.FieldName.Trim()))
                                    {
                                        if (!string.IsNullOrEmpty(item.FieldName.Trim()))
                                        {
                                            FormulaFields.Add(item.FieldName.Trim());
                                        }
                                    }
                                }

                                if (!FormulaFields.Contains(item.MatchFieldName.Trim()))
                                {
                                    if (!string.IsNullOrEmpty(item.MatchFieldName.Trim()))
                                    {
                                        FormulaFields.Add(item.MatchFieldName.Trim());
                                        FormulaField3 += "t." + item.MatchFieldName + ",";
                                    }
                                }
                            }

                            foreach (var field in FormulaFields)
                            {
                                FormulaField2 += "isnull(" + field + ",0) as " + field + ",";
                                FormulaField1 += "TransactionFieldDetails.value(''(/PayDetails/Column)[@Name = \"" + field + "\"] [1]/@Value'',''float'') as [" + field + "],";
                            }



                            foreach (var q in deductioncomponents)
                            {
                                FormulaField2 += "isnull(" + q + ",0) as " + q + ",";
                                FormulaField1 += "TransactionFieldDetails.value(''(/PayDetails/Column)[@Name = \"" + q + "\"] [1]/@Value'',''float'') as [" + q + "],";
                                FormulaField3 += "t." + q + ",";

                            }



                            if (FormulaField1 != string.Empty)
                            {
                                FormulaField1 = FormulaField1.Substring(0, FormulaField1.Length - 1);
                            }

                            if (FormulaField2 != string.Empty)
                            {
                                FormulaField2 = FormulaField2.Substring(0, FormulaField2.Length - 1);
                            }

                            if (FormulaField3 != string.Empty)
                            {
                                FormulaField3 = FormulaField3.Substring(0, FormulaField3.Length - 1);
                            }

                            string Qry = string.Empty;

                            Qry = "declare @StartDt nvarchar(max), @EndDt nvarchar(max),@ProcessDt nvarchar(max),@Company nvarchar(100) declare @field1 nvarchar(max),@field2 nvarchar(max),@field3 nvarchar(max) set @StartDt = '_start' set @EndDt = '_end' set @ProcessDt = '_process' set @Company = '_company' set @field1 = '_field1' set @field2 = '_field2' set @field3 = '_field3' " +
                         "exec('select e.Code,e.FName+ '' '' + e.LName [Name],t.ProcessDate,t.EmployeeId,' + @field3 + ' from ( select EmployeeId,ProcessDate,CompanyId,' + @field2 + '  from ( select EmployeeId,ProcessDate,CompanyId,' + @field1 + ' from dbo.EmpTransaction where CompanyId = '+ @Company + ' ) t ) t  join dbo.EmployeeDetail e on t.EmployeeId = e.Id and t.CompanyId = e.CompanyId where t.ProcessDate >= '+@StartDt+' and  e.id=[EmpId] and (t.ProcessDate <= '+@ProcessDt+' or e.EmpStatus = 1 and t.ProcessDate <= '+@EndDt+' and MONTH(e.TerminationDate) = MONTH('+@ProcessDt+')  and year(e.TerminationDate) = year('+@ProcessDt+')) order by EmployeeId,t.ProcessDate') ";


                            //   Qry = "declare @StartDt nvarchar(max), @EndDt nvarchar(max),@ProcessDt nvarchar(max),@Company nvarchar(100) declare @field1 nvarchar(max),@field2 nvarchar(max),@field3 nvarchar(max) set @StartDt = '_start' set @EndDt = '_end' set @ProcessDt = '_process' set @Company = '_company' set @field1 = '_field1' set @field2 = '_field2' set @field3 = '_field3' " +
                            //"exec('select e.Code,e.FName+ '' '' + e.LName [Name],t.ProcessDate,t.EmployeeId,' + @field3 + ' from ( select EmployeeId,ProcessDate,CompanyId,' + @field2 + '  from ( select EmployeeId,ProcessDate,CompanyId,' + @field1 + ' from dbo.EmpTransaction where CompanyId = '+ @Company + ' ) t ) t  join dbo.EmployeeDetail e on t.EmployeeId = e.Id and t.CompanyId = e.CompanyId where t.ProcessDate >= '+@StartDt+' and (t.ProcessDate <= '+@ProcessDt+' or e.EmpStatus = 1 and t.ProcessDate <= '+@EndDt+' and MONTH(e.TerminationDate) = MONTH('+@ProcessDt+')  and year(e.TerminationDate) = year('+@ProcessDt+')) order by EmployeeId,t.ProcessDate') ";

                            var Finsett = finSetData.Where(s => s.FinNo == Financialnumber).FirstOrDefault();

                            Qry = Qry.Replace("dbo", SchemaName);
                            Qry = Qry.Replace("_start", "convert(datetime,''" + Finsett.StartDate.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103)");
                            Qry = Qry.Replace("_end", "convert(datetime,''" + Finsett.EndDate.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103)");
                            Qry = Qry.Replace("_process", "convert(datetime,''" + ProcessDate_Worksheet.ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103)");
                            Qry = Qry.Replace("_company", companyId.ToString());
                            Qry = Qry.Replace("_field1", FormulaField1);
                            Qry = Qry.Replace("_field2", FormulaField2);
                            Qry = Qry.Replace("_field3", FormulaField3);
                            if (EmpId != 0)
                            {
                                Qry = Qry.Replace("[EmpId]", EmpId.ToString());
                            }
                            else
                            {
                                Qry = Qry.Replace("and  e.id=[EmpId]", "");
                            }
                            data3_worksheet = dBManager.GetDataTable(Qry, CommandType.Text);


                        }
                        var dataTran = ctx.EmpTransCustomDetails.Where(m => m.CompanyId == companyId && m.FieldName == "DESIGNATION" && m.Month == enumMonth && m.Year == strYear).Select(m => m).ToList();
                    }

                    #endregion


                    foreach (var emp in empList)
                    {


                        #region Payslip Progress
                        //if (SchemaName == "Smollfe37426078" || SchemaName == "MGMHEe93284675e" || SchemaName == "Orionb8f410a437" || SchemaName == "Total4cd8b47020")
                        if (PayslipsSchemas.Contains(SchemaName))
                        {
                            Percentagecount++;
                            BackgroundPayrollProcess processObject = new BackgroundPayrollProcess();
                            processObject.schemaName = SchemaName;
                            processObject.UserId = userId;
                            processObject.Message = "Processing " + Percentagecount + " Of " + empList.Count + " Employees";
                            processObject.PercentCount = 100 * Percentagecount / empList.Count;
                            processObject.EmployeeCount = empList.Count;
                            processObject.companyId = companyId;
                            SendPayslipNotifications(processObject, "payslip-progress");
                        }
                        #endregion


                        employeeid = emp.Id.ToString();
                        //var checkEmptrans = empTranscData.Any(a => a.EmployeeId == emp.Id && a.CompanyId == companyId && a.Month == month && a.Year == intYear);
                        var checkEmptrans = empTranscData.Any(a => a.EmployeeId == emp.Id);
                        if (checkEmptrans)
                        {
                            if (!flagPerPage && SubmitType == "Send Email")
                            {
                                // Send Emails
                                string Email = "";
                                if (recEmail == ReceiversEmail.EmailID)
                                {
                                    Email = emp.Email;
                                }
                                else if (recEmail == ReceiversEmail.PersonalEmailID)
                                {
                                    Email = emp.PersonalEmail;
                                }

                                if (string.IsNullOrEmpty(Email))
                                {
                                    if (string.IsNullOrEmpty(EmpsNotReceivedMail))
                                        EmpsNotReceivedMail = emp.Id.ToString();
                                    else
                                        EmpsNotReceivedMail += "," + emp.Id.ToString();
                                    PayslipFailedMailLog failedlog = new PayslipFailedMailLog();
                                    failedlog.EmployeeCode = emp.Code;
                                    failedlog.CreatedDate = DateTime.Now;
                                    failedlog.EmployeeName = emp.FName + " " + emp.LName;
                                    failedlog.Reason = "Email ID not set for the Employee";
                                    failedlog.UserId = userId;
                                    ListFailedLog.Add(failedlog);
                                    empListNoEmail.Add(emp);
                                    if (string.IsNullOrEmpty(EmpsNotReceivedMail))
                                        EmpsNotReceivedMail = emp.Id.ToString();
                                    else
                                        EmpsNotReceivedMail += "," + emp.Id.ToString();

                                    continue;
                                }
                            }

                            //query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,'EMP" + emp.Code + " ')";
                            //dBManager.Insert(query1, System.Data.CommandType.Text);
                            #region Check Emp Transaction
                            int templateId = 0;
                            payHeadSett = false;
                            if (payDefaultSett.Any())
                            {
                                var settType = payDefaultSett.Where(m => m.SetType == "TEMPLATE").Select(m => m.FieldValue1).FirstOrDefault();
                                var settValue = "";
                                try
                                {
                                    var businessUnit = settType.ToString().Split('_')[0];
                                    var PopUpSett = empConfig.Where(m => m.CompanyId == companyId && m.DField == "N" && m.CellType == "D").Select(m => m.FieldName).ToList();
                                    if (settType.ToString().Split('_')[0] != "")
                                    {
                                        if (PopUpSett.Contains(businessUnit))
                                        {
                                            var customBusinessUnitData = ctx.CustomeFieldDetail.Where(m => m.EmployeeId == emp.Id && m.CustomeFieldName == businessUnit).Select(m => m.FieldValue).FirstOrDefault();
                                            if (customData != null)
                                            {
                                                settValue = customBusinessUnitData;
                                            }
                                            else
                                            {
                                                settValue = "";
                                            }
                                        }
                                        else
                                        {
                                            if (businessUnit == "COMPCODE")
                                            {
                                                var empData = Util.GetPropValue(emp, "COMPANYID").ToString();
                                                settValue = empData;
                                            }
                                            else
                                            {
                                                var empData = Util.GetPropValue(emp, businessUnit).ToString();
                                                settValue = empData;
                                            }
                                        }
                                    }
                                    else
                                    {
                                        settValue = "";
                                    }
                                }
                                catch
                                {
                                    settValue = "";
                                }
                                string settFieldValue1 = settType.ToString().Split('_')[0] + "_" + settValue;
                                var settList = payDefaultSett.Where(m => m.FieldValue1 == settFieldValue1).ToList();
                                if (settList.Any())
                                {
                                    payHeadSett = settList.Count(m => m.FieldValue2 == "HEADER") == 0 ? false : true;
                                    if (payHeadSett)
                                    {
                                        templateId = int.Parse(settList.Where(m => m.FieldValue2 == "HEADER").Select(m => m.FieldValue3).FirstOrDefault());
                                        var tempPayFontSett = settList.Where(m => m.FieldValue2 == "FONT").Select(m => m.FieldValue3).FirstOrDefault();
                                        payFontSett = tempPayFontSett != null ? tempPayFontSett.ToString() : payFontSett;
                                    }
                                }
                            }
                            if (payFontSett != "")
                            {
                                if (payFontSett.ToLower() == "arial")
                                {
                                    fntTableFont = FontFactory.GetFont(BaseFont.HELVETICA, 7, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                                    fntTableFontBold = FontFactory.GetFont(BaseFont.HELVETICA, 7, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                                    fntTableFontComp = FontFactory.GetFont(BaseFont.HELVETICA, 8, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                                    fntTableFontCompBold = FontFactory.GetFont(BaseFont.HELVETICA, 8, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                                }
                                else
                                {
                                    fntTableFont = FontFactory.GetFont(payFontSett, 8, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                                    fntTableFontBold = FontFactory.GetFont(payFontSett, 8, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                                    fntTableFontComp = FontFactory.GetFont(payFontSett, 9, iTextSharp.text.Font.NORMAL, BaseColor.BLACK);
                                    fntTableFontCompBold = FontFactory.GetFont(payFontSett, 9, iTextSharp.text.Font.BOLD, BaseColor.BLACK);
                                }
                            }
                            // int PayConfigNo =   ctx.Category.Where(c => c.CompanyId == companyId && c.Id == emp.Category).Select(c => c.PayConfigNo).FirstOrDefault();
                            int PayConfigNo = lstPayConfigNo.Where(c => c.CompanyId == companyId && c.Id == emp.Category).Select(c => c.PayConfigNo).FirstOrDefault();
                            //var lstNoOrderNumber = ctx.PaySlipPrint.Where(c => c.CompanyId == companyId && c.PayConNo == PayConfigNo).Select(c => c.FieldName).ToList();
                            var lstNoOrderNumber = NoOrderNumber.Where(c => c.CompanyId == companyId && c.PayConNo == PayConfigNo).Select(c => c.FieldName).ToList();
                            //var lstPrintYesFields = ctx.AllConfig.Where(c => c.CompanyId == companyId && c.CategoryId == emp.Category && c.IncludeGross == true && !lstNoOrderNumber.Contains(c.AdditionField)).Select(c => c.LabelName).ToList();
                            var lstPrintYesFields = PrintYesFields.Where(c => c.CompanyId == companyId && c.CategoryId == emp.Category && c.IncludeGross == true && !lstNoOrderNumber.Contains(c.AdditionField)).Select(c => c.LabelName).ToList();
                            if (lstPrintYesFields.Count != 0)
                            {
                                string Fields = string.Empty;
                                foreach (var item in lstPrintYesFields)
                                {
                                    Fields += item.ToUpper() + ",";
                                }
                                if (Fields != string.Empty)
                                {
                                    Fields = Fields.Substring(0, Fields.Length - 1);
                                }
                                Msg = "error|" + Fields + " order number not given in payslip configuration ";
                                return Msg;
                                //--------------------------- viewbag error ----------------------------  ViewBag.Error = Fields + " order number not given in payslip configuration ";
                                //--------------------------- viewbag error ----------------------------   return View();
                            }
                            #region Get fields to print in middle section
                            //var vData2 = ctx.ReportMetaData.Where(a => a.ViewName == "PaySlip2" && a.CompanyId == companyId).Select(a => a).FirstOrDefault();
                            var datatable2 = new DataTable();

                            //if(emp.Code== "615015")
                            //{
                            //    query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,'EMP1 615015" + emp.Code + " ')";
                            //    dBManager.Insert(query1, System.Data.CommandType.Text);

                            //}

                            //if (SchemaName != "Smollfe37426078" && SchemaName != "MGMHEe93284675e" && SchemaName != "Orionb8f410a437" && SchemaName != "Total4cd8b47020")
                            if (!PayslipsSchemas.Contains(SchemaName))

                            {
                                string conData2 = vData2.Query.Replace("dbo", SchemaName);
                                conData2 = conData2.Replace("_configNo", PayConfigNo.ToString());
                                conData2 = conData2.Replace("_month", Month);
                                conData2 = conData2.Replace("_year", Year);
                                conData2 = conData2.Replace("_company", companyId.ToString());
                                conData2 = conData2.Replace("_employee", emp.Id.ToString());
                                datatable2 = dBManager.GetDataTable(conData2, CommandType.Text);
                            }
                            else
                            {
                                //datatable2 = payData1.AsEnumerable()
                                //         .Where(row => row.Field<int>("Id") == emp.Id).CopyToDataTable();

                                datatable2 = payData1.Select("Id =" + emp.Id).CopyToDataTable();
                            }

                            #endregion
                            //var vData = ctx.ReportMetaData.Where(a => a.ViewName == "PaySlip" && a.CompanyId == companyId).Select(a => a).FirstOrDefault();
                            var datatable = new DataTable();
                            PayslipPassword passFlag;

                            //if (SchemaName != "Smollfe37426078" && SchemaName != "MGMHEe93284675e" && SchemaName != "Orionb8f410a437" && SchemaName != "Total4cd8b47020")
                            if (!PayslipsSchemas.Contains(SchemaName))
                            {
                                conData = vData.Query.Replace("dbo", SchemaName);
                                conData = conData.Replace("_configNo", PayConfigNo.ToString());
                                conData = conData.Replace("_month", Month);
                                conData = conData.Replace("_year", Year);
                                conData = conData.Replace("_company", companyId.ToString());
                                conData = conData.Replace("_employee", emp.Id.ToString());
                                datatable = dBManager.GetDataTable(conData, CommandType.Text);

                            }
                            else
                            {
                                //datatable = payData2.AsEnumerable()
                                //            .Where(row => row.Field<int>("Id") == emp.Id).CopyToDataTable();

                                datatable = payData2.Select("Id =" + emp.Id).CopyToDataTable();

                            }
                            //if (emp.Code == "615015")
                            //{
                            //    query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,'EMP2 615015" + emp.Code + " ')";
                            //    dBManager.Insert(query1, System.Data.CommandType.Text);

                            //}
                            //var dataOtherSett = ctx.PaySlipOtherSetting.Where(t => t.CompanyId == companyId && t.PayConNo == PayConfigNo).FirstOrDefault();
                            var dataOtherSett = lstdataOtherSett.Where(t => t.CompanyId == companyId && t.PayConNo == PayConfigNo).FirstOrDefault();
                            //&& SubmitType != "Download" - Smollan Require password in all pags
                            if (dataOtherSett != null && dataOtherSett.PayslipPassword != 0)
                            {
                                //if (form["isSelfServie"] == "False")
                                //    passFlag = 0;
                                //else
                                passFlag = dataOtherSett.PayslipPassword;
                            }
                            else
                            {
                                passFlag = 0;
                            }
                            //Password Apply on Setttnig >> 
                            if (dataOtherSett != null)
                            {
                                var passwordApply = dataOtherSett.PasswordApply.ToString().Split(',');
                                //if (form["isSelfServie"].ToString().ToLower() == "true")
                                //{
                                //    //SelfService
                                //    if (!passwordApply.Contains("2"))
                                //    {
                                //        passFlag = 0;
                                //    }
                                //}
                                //else
                                //{
                                if (SubmitType == "Send Email")
                                {
                                    //Email
                                    if (!passwordApply.Contains("3"))
                                    {
                                        passFlag = 0;
                                    }
                                }
                                else if (!passwordApply.Contains("1"))
                                {
                                    //cloud
                                    passFlag = 0;
                                }
                                //}
                            }
                            //  << Password Apply on Setttnig 
                            OuterTable = new PdfPTable(6);
                            OuterTable.WidthPercentage = 100;
                            MainTable = new PdfPTable(6);
                            MainTable.WidthPercentage = 100;
                            EarningTable = new PdfPTable(3);
                            EarningTable.WidthPercentage = 100;
                            EarningTable.SetWidths(new float[] { 5.5f, 3f, 3f });
                            DeductionTable = new PdfPTable(3);
                            DeductionTable.WidthPercentage = 100;
                            DeductionTable.SetWidths(new float[] { 6f, 2f, 2f });
                            #region Company Logo, Name, Address - Box 1
                            List<string> lstSchemaName = new List<string>();
                            if (!string.IsNullOrEmpty(schemaNames))
                            {
                                lstSchemaName = schemaNames.Split(',').ToList();
                            }
                            if (!payHeadSett)
                            {
                                if (dataOtherSett != null && dataOtherSett.PrintCom != false)
                                {
                                    string compAddress = String.IsNullOrEmpty(dataComp.Address) == true ? "" : dataComp.Address;
                                    compAddress += String.IsNullOrEmpty(dataComp.City) == true ? "" : ", " + dataComp.City;
                                    compAddress += String.IsNullOrEmpty(dataComp.State) == true ? "" : ", " + dataComp.State;
                                    compAddress += String.IsNullOrEmpty(dataComp.PinCode) == true ? "" : "-" + dataComp.PinCode;
                                    //Path will store Payslip Logo Path
                                    //bool isLogoExists = false;
                                    //string path = "";
                                    if (lstSchemaName.Contains(SchemaName))
                                    {
                                        if (SchemaName == "Aquara7bc772839")
                                        {
                                            //New to get logo from local system 
                                            path = payslipPath + "//Images//Aquara7bc772839//";
                                            path += "ac-logo.jpg";
                                            isLogoExists = true;
                                            if (!File.Exists(payslipPath + "//Images//Aquara7bc772839//ac-logo.jpg"))
                                            {
                                                isLogoExists = false;
                                            }
                                        }
                                        else if (SchemaName == "Brill0bde97f2e2")
                                        {
                                            //New to get logo from local system 
                                            path = payslipPath + "//Images//Brill0bde97f2e2//";
                                            isLogoExists = true;
                                            switch (companyId)
                                            {
                                                case 1:
                                                    path += "1-logo.jpg";
                                                    break;
                                                case 2:
                                                    path += "2-logo.jpg";
                                                    break;
                                                case 3:
                                                    path += "3-logo.jpg";
                                                    break;
                                                case 4:
                                                    path += "4-logo.jpg";
                                                    break;
                                                default:
                                                    path += "1-logo.jpg";
                                                    break;
                                            }
                                            if (!File.Exists(payslipPath + "//Images//Brill0bde97f2e2//" + companyId + "-logo.jpg"))
                                            {
                                                isLogoExists = false;
                                            }
                                        }
                                        else
                                        {
                                            // New to get logo from local system 
                                            path = payslipPath + "//Images//" + SchemaName + "//";
                                            path += "ac-logo.jpg";
                                            isLogoExists = true;
                                            if (!File.Exists(payslipPath + "//Images//" + SchemaName + "//ac-logo.jpg"))
                                            {
                                                isLogoExists = false;
                                            }
                                        }
                                    }
                                    //else
                                    //{
                                    //    string cs = new StaticConfigs().GetServerConnection(ConfigurationType.BlobURL);
                                    //    path = cs + "/companylogo-" + SchemaName.ToLower() + "/";
                                    //    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(new StaticConfigs().GetServerConnection(ConfigurationType.BlobConnectionString));// DevelopmentStorageAccount;
                                    //    CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                                    //    CloudBlobContainer container = blobClient.GetContainerReference("companylogo" + "-" + SchemaName.ToLower());
                                    //    if (container.Exists())
                                    //    {
                                    //        CloudBlockBlob blockBlob = container.GetBlockBlobReference(SchemaName + "-" + companyId);
                                    //        if (blockBlob.Exists())
                                    //        {
                                    //            isLogoExists = true;
                                    //            path += SchemaName + "-" + companyId;
                                    //            //return Json(new { data = path, flag = true }, JsonRequestBehavior.AllowGet);
                                    //        }
                                    //    }
                                    //}

                                    if (isLogoExists)
                                    {
                                        PdfPTable ImageTable = new PdfPTable(6);
                                        ImageTable.WidthPercentage = 100;
                                        PdfPTable CompanyTable = new PdfPTable(6);
                                        CompanyTable.WidthPercentage = 100;
                                        //if (SchemaName == "Smollfe37426078")
                                        //{
                                        //    var query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,' " + companyPath + " ')";
                                        //    dBManager.Insert(query1, System.Data.CommandType.Text);
                                        //}
                                        // iTextSharp.text.Image image = iTextSharp.text.Image.GetInstance(path);
                                        iTextSharp.text.Image image = iTextSharp.text.Image.GetInstance(companyPath);

                                        if (SchemaName == "Aquara7bc772839")
                                        {
                                            image.ScalePercent(26f);
                                        }
                                        else
                                        {
                                            image.ScalePercent(57f);
                                        }
                                        image.IndentationLeft = 27f;
                                        image.Alignment = iTextSharp.text.Image.TEXTWRAP | iTextSharp.text.Image.ALIGN_LEFT;
                                        PdfPCell imageCell = new PdfPCell(image);
                                        imageCell.Colspan = 6;
                                        imageCell.HorizontalAlignment = Element.ALIGN_LEFT;
                                        imageCell.BorderWidth = 0;
                                        imageCell.PaddingLeft = 1f;
                                        ImageTable.AddCell(imageCell);
                                        //PdfPCell cellComp = new PdfPCell(new Phrase(dataComp.CompanyName, fntTableFontBold));
                                        //cellComp.Colspan = 5;
                                        // PdfPCell cellComp = new PdfPCell(new Phrase(dataComp.CompanyName, fntTableFontCompBold));
                                        PdfPCell cellComp = new PdfPCell(new Phrase(dataComp.CompanyName, fntTableFontCompBold11));
                                        cellComp.Colspan = 6;
                                        cellComp.HorizontalAlignment = Element.ALIGN_CENTER;
                                        cellComp.BorderWidth = 0;
                                        if (SchemaName == "Xylemd237502c94")
                                        {
                                            cellComp.PaddingTop = 0F;
                                        }
                                        else
                                        {
                                            cellComp.PaddingTop = 12F;
                                        }
                                        cellComp.PaddingLeft = -70F;
                                        CompanyTable.AddCell(cellComp);
                                        cellComp = new PdfPCell(new Phrase(compAddress, fntTableFontComp));
                                        cellComp.Colspan = 6; // 5 for Image
                                        cellComp.HorizontalAlignment = Element.ALIGN_CENTER;
                                        cellComp.BorderWidth = 0;
                                        cellComp.PaddingLeft = -70F;
                                        CompanyTable.AddCell(cellComp);
                                        if (SchemaName == "Xylemd237502c94")
                                        {
                                            PdfPCell cellCINNO = new PdfPCell(new Phrase(" ", fntTableFontComp));
                                            cellCINNO = new PdfPCell(new Phrase("CIN No : U74999PN2009PTC134592", fntTableFontComp));
                                            cellCINNO.Colspan = 6;
                                            cellCINNO.BorderWidth = 0;
                                            cellCINNO.HorizontalAlignment = Element.ALIGN_CENTER;
                                            cellCINNO.PaddingLeft = -70F;
                                            CompanyTable.AddCell(cellCINNO);
                                        }
                                        PdfPCell cellPaySlip = new PdfPCell(new Phrase(" ", fntTableFontComp));
                                        if (dataOtherSett != null && dataOtherSett.MonthString != null)
                                        {
                                            // cellPaySlip = new PdfPCell(new Phrase("(" + dataOtherSett.MonthString + " " + month.ToString().ToUpper() + " " + year + ")", fntTableFontComp));
                                            cellPaySlip = new PdfPCell(new Phrase(dataOtherSett.MonthString + " " + month.ToString() + " " + year, fntTableFontCompBold88));
                                        }
                                        else
                                        {
                                            // cellPaySlip = new PdfPCell(new Phrase(month.ToString().ToUpper() + " " + year, fntTableFontCompBold88));
                                            cellPaySlip = new PdfPCell(new Phrase(month.ToString().ToUpper() + " " + year, fntTableFontCompBold88));
                                        }
                                        cellPaySlip.Colspan = 6;
                                        cellPaySlip.BorderWidth = 0;
                                        cellPaySlip.HorizontalAlignment = Element.ALIGN_CENTER;
                                        cellPaySlip.PaddingBottom = 6F;
                                        cellPaySlip.PaddingLeft = -70F;
                                        CompanyTable.AddCell(cellPaySlip);
                                        PdfPCell cellLeft = new PdfPCell();
                                        cellLeft.Colspan = 1;
                                        cellLeft.AddElement(ImageTable);
                                        cellLeft.BackgroundColor = BaseColor.WHITE;
                                        cellLeft.BorderColor = BaseColor.BLACK;
                                        cellLeft.BorderWidthBottom = 0.5F;
                                        cellLeft.BorderWidthTop = 0F;
                                        cellLeft.BorderWidthLeft = 0;
                                        cellLeft.BorderWidthRight = 0;
                                        MainTable.AddCell(cellLeft);
                                        PdfPCell cellRight = new PdfPCell();
                                        cellRight.Colspan = 5;
                                        cellRight.AddElement(CompanyTable);
                                        cellRight.BackgroundColor = BaseColor.WHITE;
                                        cellRight.BorderColor = BaseColor.BLACK;
                                        cellRight.BorderWidthBottom = 0.5F;
                                        cellRight.BorderWidthTop = 0F;
                                        cellRight.BorderWidthLeft = 0;
                                        cellRight.BorderWidthRight = 0;
                                        MainTable.AddCell(cellRight);
                                    }
                                    else
                                    {
                                        // PdfPCell cellComp = new PdfPCell(new Phrase(dataComp.CompanyName, fntTableFontCompBold));
                                        PdfPCell cellComp = new PdfPCell(new Phrase(dataComp.CompanyName, fntTableFontCompBold11));
                                        cellComp.Colspan = 6;
                                        cellComp.HorizontalAlignment = Element.ALIGN_CENTER;
                                        cellComp.BorderWidth = 0;
                                        MainTable.AddCell(cellComp);
                                        cellComp = new PdfPCell(new Phrase(compAddress, fntTableFontComp));
                                        cellComp.Colspan = 6; // 5 for Image
                                        cellComp.HorizontalAlignment = Element.ALIGN_CENTER;
                                        cellComp.BorderWidth = 0;
                                        MainTable.AddCell(cellComp);
                                        PdfPCell cellPaySlip = new PdfPCell(new Phrase(" ", fntTableFontComp));
                                        if (dataOtherSett != null && dataOtherSett.MonthString != null)
                                        {
                                            cellPaySlip = new PdfPCell(new Phrase(dataOtherSett.MonthString + " " + month.ToString() + " " + year, fntTableFontCompBold88));
                                        }
                                        // fntTableFontCompBold
                                        else
                                        {
                                            // cellPaySlip = new PdfPCell(new Phrase(month.ToString() + " " + year, fntTableFontComp));
                                            cellPaySlip = new PdfPCell(new Phrase(month.ToString() + " " + year, fntTableFontCompBold88));
                                        }
                                        cellPaySlip.Colspan = 6;
                                        cellPaySlip.HorizontalAlignment = Element.ALIGN_CENTER;
                                        cellPaySlip.BorderWidth = 0;
                                        cellPaySlip.PaddingBottom = 6F;
                                        cellPaySlip.BackgroundColor = BaseColor.WHITE;
                                        cellPaySlip.BorderColor = BaseColor.BLACK;
                                        cellPaySlip.BorderWidthBottom = 0.5F;
                                        cellPaySlip.BorderWidthTop = 0F;
                                        cellPaySlip.BorderWidthLeft = 0;
                                        MainTable.AddCell(cellPaySlip);
                                    }
                                    //Original Without Logo
                                    //PdfPCell cellComp = new PdfPCell(new Phrase(dataComp.CompanyName, fntTableFont));
                                    //cellComp.Colspan = 6;
                                    //cellComp.HorizontalAlignment = Element.ALIGN_CENTER;
                                    //cellComp.BorderWidth = 0;
                                    //MainTable.AddCell(cellComp);
                                    //cellComp = new PdfPCell(new Phrase(compAddress, fntTableFont));
                                    //cellComp.Colspan = 6; // 5 for Image
                                    //cellComp.HorizontalAlignment = Element.ALIGN_CENTER;
                                    //cellComp.BorderWidth = 0;
                                    //MainTable.AddCell(cellComp);
                                }
                            }
                            #endregion
                            #region Printing fields in header section - Box 2
                            // ------------ Header Section - Starts
                            PdfPCell cellHead = new PdfPCell(new Phrase(" ", fntTableFont));
                            PdfPCell cellEmpty = new PdfPCell(new Phrase(" ", fntTableFont));
                            if (!payHeadSett)
                            {
                                // Rule
                                if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Rule))
                                {
                                    PdfPCell cellRule = new PdfPCell(new Phrase(dataOtherSett.Rule, fntTableFont));
                                    cellRule.Colspan = 6;
                                    cellRule.BorderWidth = 0;
                                    MainTable.AddCell(cellRule);
                                }
                                //var HeaderFields = ctx.PaySlipSetting.Where(p => p.PayConNo == PayConfigNo && p.HeaderPrintOrder != 0 && p.CompanyId == companyId).OrderBy(p => p.HeaderPrintOrder).ToList();
                                var HeaderFields = lstHeaderFields.Where(p => p.PayConNo == PayConfigNo && p.HeaderPrintOrder != 0 && p.CompanyId == companyId).OrderBy(p => p.HeaderPrintOrder).ToList();
                                int tempcounterHeader = 0;
                                #region Transaction Logic
                                List<EmpTransCustomDetails> dataTran = new List<EmpTransCustomDetails>();
                                if (HeaderFields.Any(m => m.ComponentType == ComponentType.ManualFields))
                                {
                                    dataTran = TransactionData.Where(m => m.CompanyId == companyId && m.EmployeeId == emp.Id && m.Month == enumMonth && m.Year == strYear).Select(m => m).ToList();
                                }
                                #endregion
                                foreach (var masterfields in HeaderFields)
                                {
                                    if (masterfields.ComponentType == ComponentType.ManualFields)
                                    {
                                        string finalValue = string.Empty;
                                        var tempConfig = empConfig.Where(m => m.FieldName == masterfields.FieldName).FirstOrDefault();
                                        Dictionary<bool, string> returnDic = _employeeRepository.getEmpTransCustomDetails(emp.Id, tempConfig, dataTran, dataCombo, lstPayConfigNo, month, year);
                                        if (returnDic.ElementAt(0).Key)
                                        {
                                            finalValue = returnDic.ElementAt(0).Value;
                                        }
                                        else
                                        {
                                            finalValue = _employeeRepository.GetEmpDataFieldWise(SchemaName, companyId, emp.Id, masterfields.FieldName, masterfields.LabelName, emp, dataComp, lstPayConfigNo, empConfig, dataCombo, customData);
                                        }
                                        if (masterfields.FieldName == "FNAME")
                                        {
                                            // cellHead = new PdfPCell(new Phrase(masterfields.LabelName + " : ", fntTableFontBold));
                                            cellHead = new PdfPCell(new Phrase(masterfields.LabelName + " : ", fntTableFont));
                                        }
                                        else
                                        {
                                            cellHead = new PdfPCell(new Phrase(masterfields.LabelName + " : ", fntTableFont));
                                        }
                                        cellHead.BorderWidth = 0;
                                        cellHead.PaddingLeft = 10F;
                                        cellHead.PaddingTop = 3F;
                                        cellHead.PaddingBottom = 3F;
                                        MainTable.AddCell(cellHead);
                                        if (masterfields.FieldName == "CODE" || masterfields.FieldName == "FNAME")
                                        {
                                            // cellHead = new PdfPCell(new Phrase(finalValue, fntTableFontBold));
                                            cellHead = new PdfPCell(new Phrase(finalValue, fntTableFont));
                                        }
                                        else if (masterfields.FieldName == "REGIMETYPE")
                                        {
                                            if (finalValue == "OLD1920")
                                            {
                                                cellHead = new PdfPCell(new Phrase("Old Regime", fntTableFont));
                                            }
                                            else
                                            {
                                                cellHead = new PdfPCell(new Phrase("New Regime", fntTableFont));
                                            }
                                        }
                                        else
                                        {
                                            cellHead = new PdfPCell(new Phrase(finalValue, fntTableFont));
                                        }
                                        cellHead.Colspan = 2;
                                        cellHead.BorderWidth = 0;
                                        cellHead.PaddingLeft = 10F;
                                        cellHead.PaddingTop = 3F;
                                        cellHead.PaddingBottom = 3F;
                                        MainTable.AddCell(cellHead);
                                        tempcounterHeader++;
                                    }
                                    else
                                    {
                                        DataRow[] dr = datatable.Select(" FieldName = '" + masterfields.FieldName + "'");
                                        if (!string.IsNullOrEmpty(Supress))
                                        {
                                            double value = string.IsNullOrEmpty(dr[0]["Amount"].ToString()) ? 0 : double.Parse(dr[0]["Amount"].ToString());
                                            if (value != 0)
                                            {
                                                cellHead = new PdfPCell(new Phrase(dr[0]["LabelName"] + " : ", fntTableFont));
                                                cellHead.BorderWidth = 0;
                                                //
                                                cellHead.PaddingLeft = 10F;
                                                cellHead.PaddingTop = 3F;
                                                cellHead.PaddingBottom = 3F;
                                                cellHead.PaddingRight = -5F;
                                                MainTable.AddCell(cellHead);
                                                cellHead = new PdfPCell(new Phrase(value.ToString("0.00"), fntTableFont));
                                                cellHead.Colspan = 2;
                                                cellHead.BorderWidth = 0;
                                                //
                                                cellHead.PaddingLeft = 10F;
                                                cellHead.PaddingTop = 3F;
                                                cellHead.PaddingBottom = 3F;
                                                cellHead.PaddingRight = -5F;
                                                MainTable.AddCell(cellHead);
                                                tempcounterHeader++;
                                            }
                                        }
                                        else
                                        {
                                            cellHead = new PdfPCell(new Phrase(dr[0]["LabelName"] + " : ", fntTableFont));
                                            cellHead.BorderWidth = 0;
                                            cellHead.PaddingLeft = 10F;
                                            cellHead.PaddingTop = 3F;
                                            cellHead.PaddingBottom = 3F;
                                            cellHead.PaddingRight = -5F;
                                            MainTable.AddCell(cellHead);
                                            double value = string.IsNullOrEmpty(dr[0]["Amount"].ToString()) ? 0 : double.Parse(dr[0]["Amount"].ToString());
                                            //cellHead = new PdfPCell(new Phrase((double.Parse(dr[0]["Amount"].ToString()) == 0 ? " " : double.Parse(dr[0]["Amount"].ToString()).ToString("0.00")), fntTableFont));
                                            cellHead = new PdfPCell(new Phrase(value.ToString("0.00"), fntTableFont));
                                            cellHead.Colspan = 2;
                                            cellHead.BorderWidth = 0;
                                            // 
                                            cellHead.PaddingLeft = 10F;
                                            cellHead.PaddingTop = 3F;
                                            cellHead.PaddingBottom = 3F;
                                            cellHead.PaddingRight = -5F;
                                            MainTable.AddCell(cellHead);
                                            tempcounterHeader++;
                                        }
                                    }
                                }
                                if ((tempcounterHeader % 2) != 0)
                                {
                                    cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                    cellHead.Colspan = 3;
                                    cellHead.BorderWidth = 0;
                                    //
                                    cellHead.PaddingLeft = 10F;
                                    cellHead.PaddingTop = 3F;
                                    cellHead.PaddingBottom = 3F;
                                    MainTable.AddCell(cellHead);
                                }
                                // ------------ Header Section - Ends
                            }
                            #endregion
                            #region Earnings & Deductions - Box 3
                            YTDFlag = false;
                            PdfPCell cell = new PdfPCell(new Phrase("EARNINGS", fntTableFontBold));
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            if (payHeadSett)
                            {
                                cell.BorderWidthTop = 0;
                            }
                            else
                            {
                                cell.BorderWidthTop = 0.5F;
                            }
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            cell.HorizontalAlignment = Element.ALIGN_LEFT;
                            cell.PaddingLeft = 10F;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            MainTable.AddCell(cell);
                            if (dataOtherSett.MatchOrCumm == MatchOrCumm.Matching || dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                            {
                                // Only For Commulative Setting
                                if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                                {
                                    YTDFlag = true;
                                }
                                cell = new PdfPCell(new Phrase(dataOtherSett.ActualLabel, fntTableFontBold));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                cell.PaddingLeft = 10F;
                                MainTable.AddCell(cell);
                                cell = new PdfPCell(new Phrase(dataOtherSett.ComputeLabel, fntTableFontBold));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                cell.PaddingRight = 10F;
                                MainTable.AddCell(cell);
                            }
                            else
                            {
                                cell = new PdfPCell(new Phrase("", fntTableFont));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                MainTable.AddCell(cell);
                                cell = new PdfPCell(new Phrase("AMOUNT", fntTableFontBold));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                if (SchemaName == "Prove923458c2c1")
                                {
                                    cell.BorderWidthRight = 0.5F;
                                }
                                else
                                {
                                    cell.BorderWidthRight = 0;
                                }
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                cell.PaddingRight = 10F;
                                MainTable.AddCell(cell);
                            }
                            cell = new PdfPCell(new Phrase("DEDUCTIONS", fntTableFontBold));
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            if (payHeadSett)
                            {
                                cell.BorderWidthTop = 0;
                            }
                            else
                            {
                                cell.BorderWidthTop = 0.5F;
                            }
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            cell.HorizontalAlignment = Element.ALIGN_LEFT;
                            cell.PaddingLeft = 10F;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            MainTable.AddCell(cell);
                            if (dataOtherSett.MatchOrCumm == MatchOrCumm.Matching)
                            {
                                // cell = new PdfPCell(new Phrase(dataOtherSett.ActualLabel, fntTableFontBold));
                                cell = new PdfPCell(new Phrase(""));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                cell.PaddingLeft = 10F;
                                MainTable.AddCell(cell);
                                //cell = new PdfPCell(new Phrase(dataOtherSett.ComputeLabel, fntTableFontBold));
                                cell = new PdfPCell(new Phrase("AMOUNT", fntTableFontBold));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                cell.PaddingRight = 10F;
                                cell.PaddingLeft = 5F;
                                MainTable.AddCell(cell);
                            }
                            else if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                            {
                                cell = new PdfPCell(new Phrase(dataOtherSett.ActualLabel, fntTableFontBold));
                                // cell = new PdfPCell(new Phrase(""));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                cell.PaddingLeft = 10F;
                                MainTable.AddCell(cell);
                                cell = new PdfPCell(new Phrase(dataOtherSett.ComputeLabel, fntTableFontBold));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                // cell.PaddingRight = 10F;
                                cell.PaddingRight = 5F;
                                MainTable.AddCell(cell);
                            }
                            else
                            {
                                cell = new PdfPCell(new Phrase("", fntTableFont));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                MainTable.AddCell(cell);
                                cell = new PdfPCell(new Phrase("AMOUNT", fntTableFontBold));
                                cell.BackgroundColor = BaseColor.WHITE;
                                cell.BorderColor = BaseColor.BLACK;
                                cell.BorderWidthBottom = 0.5F;
                                if (payHeadSett)
                                {
                                    cell.BorderWidthTop = 0;
                                }
                                else
                                {
                                    cell.BorderWidthTop = 0.5F;
                                }
                                cell.BorderWidthLeft = 0;
                                cell.BorderWidthRight = 0;
                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                cell.PaddingTop = 3F;
                                cell.PaddingBottom = 3F;
                                MainTable.AddCell(cell);
                            }
                            #region Printing fields in middle section
                            DataView dv = datatable2.DefaultView;
                            dv.Sort = "PrintOrder asc";
                            DataTable sortedDT = dv.ToTable();
                            //datatable2.DefaultView.Sort = "PrintOrder asc";
                            //DataRow[] Earnings = datatable2.Select("Flag = 1");
                            //DataRow[] Deductions = datatable2.Select("Flag = 0");
                            //Update By Giving Print Order
                            DataRow[] Earnings = sortedDT.Select("Flag = 1");
                            DataRow[] Deductions = sortedDT.Select("Flag = 0");
                            //table.DefaultView.Sort = "columnName asc";
                            float EarnTotal = 0, EarnMatchTot = 0;
                            float DedTotal = 0, DedMatchTot = 0; ;
                            maxcounter = Earnings.Length > Deductions.Length ? Earnings.Length : Deductions.Length;
                            #region Cumulative option -  Nilesh 11/05/2016
                            //*** Cumulative Option in Payslip ***//
                            if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                            {
                                lstmodel = new List<EmpTransCustomDetails>();
                                DateTime startDate = PayDate;
                                if ((int)dataOtherSett.CummMonth > PayDate.Month)
                                {
                                    startDate = new DateTime(PayDate.Year - 1, (int)dataOtherSett.CummMonth, 1);
                                }
                                else
                                {
                                    startDate = new DateTime(PayDate.Year, (int)dataOtherSett.CummMonth, 1);
                                }
                                var transactionData = transactionDataLst.Where(a => a.CompanyId == companyId && a.EmployeeId == emp.Id && a.ProcessDate >= startDate && a.ProcessDate <= PayDate).Select(a => a).ToList();
                                foreach (var item in transactionData)
                                {
                                    //XmlDocument xDoc = new XmlDocument();
                                    //if (transactionData != null)
                                    //{
                                    //    xDoc.LoadXml(item.TransactionFieldDetails);
                                    //}
                                    //XmlNodeList xnList = xDoc.SelectNodes("/PayDetails/Column");
                                    //foreach (XmlNode xn in xnList)
                                    //{
                                    //    lstmodel.Add(new EmpTransCustomDetails() { FieldName = xn.Attributes["Name"].Value, FieldValue = double.Parse(xn.Attributes["Value"].Value) });
                                    //}

                                    Dictionary<string, double> transactionFields = new Dictionary<string, double>();
                                    if (transactionData != null && !string.IsNullOrEmpty(item.TransactionFieldDetailsJson))
                                    {
                                        transactionFields = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, double>>(item.TransactionFieldDetailsJson);
                                    }
                                    foreach (var feild in transactionFields)
                                    {
                                        lstmodel.Add(new EmpTransCustomDetails() { FieldName = feild.Key.ToString(), FieldValue = feild.Value });
                                    }
                                }
                            }
                            //***********************************//
                            #endregion
                            for (int i = 0; i < maxcounter; i++)
                            {
                                double MatchValue = 0;
                                double ActualValue = 0;
                                if (Earnings.Length > i)
                                {
                                    if (!string.IsNullOrEmpty(Supress))
                                    {
                                        MatchValue = string.IsNullOrEmpty(Earnings[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Earnings[i]["Match"].ToString());
                                        ActualValue = string.IsNullOrEmpty(Earnings[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Earnings[i]["Amount"].ToString());
                                        if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                                        {
                                            var data = lstmodel.Where(a => a.FieldName == Earnings[i]["FieldName"].ToString()).Sum(a => a.FieldValue);
                                            MatchValue = string.IsNullOrEmpty(Earnings[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Earnings[i]["Amount"].ToString());
                                            ActualValue = data;
                                        }
                                        if (MatchValue != 0 || ActualValue != 0)
                                        {
                                            cell = new PdfPCell(new Phrase(Earnings[i]["LabelName"].ToString(), fntTableFont));
                                            // cell.PaddingLeft = 10F;
                                            cell.PaddingLeft = 8F;
                                            cell.BorderWidth = 0;
                                            EarningTable.AddCell(cell);
                                            double matchval = String.IsNullOrEmpty(MatchValue.ToString()) ? 0 : double.Parse(MatchValue.ToString());
                                            cell = new PdfPCell(new Phrase(matchval == 0 ? " " : matchval.ToString("0.00"), fntTableFont));
                                            cell.BorderWidth = 0;
                                            cell.PaddingLeft = 0;
                                            cell.PaddingRight = 20F;
                                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                            EarningTable.AddCell(cell);
                                            double val = String.IsNullOrEmpty(ActualValue.ToString()) ? 0 : double.Parse(ActualValue.ToString());
                                            cell = new PdfPCell(new Phrase(val == 0 ? " " : val.ToString("0.00"), fntTableFont));
                                            cell.BorderWidth = 0;
                                            cell.PaddingRight = 10F;
                                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                            EarningTable.AddCell(cell);
                                            cnt++;
                                        }
                                    }
                                    else
                                    {
                                        MatchValue = string.IsNullOrEmpty(Earnings[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Earnings[i]["Match"].ToString());
                                        ActualValue = string.IsNullOrEmpty(Earnings[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Earnings[i]["Amount"].ToString());
                                        if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                                        {
                                            var data = lstmodel.Where(a => a.FieldName == Earnings[i]["FieldName"].ToString()).Sum(a => a.FieldValue);
                                            MatchValue = string.IsNullOrEmpty(Earnings[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Earnings[i]["Amount"].ToString());
                                            ActualValue = data;
                                        }

                                        cell = new PdfPCell(new Phrase(Earnings[i]["LabelName"].ToString(), fntTableFont));
                                        cell.BorderWidth = 0;
                                        // cell.PaddingLeft = 10F;
                                        cell.PaddingLeft = 8F;
                                        EarningTable.AddCell(cell);
                                        double MatchVal = string.IsNullOrEmpty(Earnings[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Earnings[i]["Match"].ToString());
                                        //cell = new PdfPCell(new Phrase(double.Parse(Earnings[i]["Match"].ToString()) == 0 ? " " : double.Parse(Earnings[i]["Match"].ToString()).ToString("0.00"), fntTableFont));
                                        cell = new PdfPCell(new Phrase(MatchVal == 0 ? " " : MatchVal.ToString("0.00"), fntTableFont));
                                        cell.BorderWidth = 0;
                                        cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                        EarningTable.AddCell(cell);
                                        double val = String.IsNullOrEmpty(Earnings[i]["Amount"].ToString()) ? 0 : double.Parse(Earnings[i]["Amount"].ToString());
                                        cell = new PdfPCell(new Phrase(val == 0 ? " " : val.ToString("0.00"), fntTableFont));
                                        cell.BorderWidth = 0;
                                        cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                        cell.PaddingRight = 10F;
                                        EarningTable.AddCell(cell);
                                    }
                                    EarnTotal += float.Parse(string.IsNullOrEmpty(ActualValue.ToString()) ? "0" : ActualValue.ToString("0.00"));
                                    EarnMatchTot += float.Parse(string.IsNullOrEmpty(MatchValue.ToString()) ? "0" : MatchValue.ToString("0.00"));
                                }
                                if (Deductions.Length > i)
                                {
                                    MatchValue = 0;
                                    ActualValue = 0;
                                    bool flag1 = false;
                                    if (!string.IsNullOrEmpty(Supress))
                                    {
                                        MatchValue = string.IsNullOrEmpty(Deductions[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Match"].ToString());
                                        ActualValue = string.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Amount"].ToString());
                                        if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                                        {
                                            var data = lstmodel.Where(a => a.FieldName == Deductions[i]["FieldName"].ToString()).Sum(a => a.FieldValue);
                                            MatchValue = string.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Amount"].ToString());
                                            ActualValue = data;
                                        }
                                        if (MatchValue != 0 || ActualValue != 0)
                                        {
                                            if (listLoanMaster.Any(a => a.LoanCode == Deductions[i]["FieldName"].ToString()))
                                            {

                                                string noOfInstallment = string.Empty;
                                                string loancode = Deductions[i]["FieldName"].ToString();
                                                if (listLoanMaster.Any(a => a.LoanCode == loancode && a.InterestLogic == InterestLogic.EMI_With_Interest))
                                                {
                                                    flag1 = true;
                                                }
                                                var noIstallmentData = LoanInstallment.Where(a => a.EmployeeId == emp.Id && a.MonthlyDate == PayDate && a.LoanCode == loancode && a.FCStatus != "Y").ToList();
                                                if (noIstallmentData.Any())
                                                {
                                                    DateTime tempDate = noIstallmentData.Select(a => a.ApplyDate).FirstOrDefault();//noIstallmentData[0].ApplyDate;
                                                    var noIstallmentData1 = await ctx.MonthLoan.Where(a => a.EmployeeId == emp.Id && a.ApplyDate == tempDate && a.LoanCode == loancode && a.FCStatus != "Y").ToListAsync();
                                                    int countIstallment = noIstallmentData1.Count(a => a.MonthlyDate <= PayDate);
                                                    noOfInstallment = "(" + countIstallment.ToString() + "/" + noIstallmentData1.Count.ToString() + ")";
                                                }
                                                cell = new PdfPCell(new Phrase(Deductions[i]["LabelName"].ToString() + " " + noOfInstallment, fntTableFont));
                                            }
                                            else
                                            {
                                                cell = new PdfPCell(new Phrase(Deductions[i]["LabelName"].ToString(), fntTableFont));
                                            }
                                            cell.BorderWidth = 0;
                                            // cell.PaddingLeft = 10F;
                                            cell.PaddingLeft = 8F;
                                            DeductionTable.AddCell(cell);
                                            cell = new PdfPCell(new Phrase(MatchValue == 0 ? " " : MatchValue.ToString("0.00"), fntTableFont));
                                            cell.BorderWidth = 0;
                                            cell.PaddingRight = 20F;
                                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                            DeductionTable.AddCell(cell);
                                            double val = String.IsNullOrEmpty(ActualValue.ToString()) ? 0 : double.Parse(ActualValue.ToString());
                                            cell = new PdfPCell(new Phrase(val == 0 ? " " : val.ToString("0.00"), fntTableFont));
                                            cell.BorderWidth = 0;
                                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                            DeductionTable.AddCell(cell);
                                        }
                                    }
                                    else
                                    {
                                        MatchValue = string.IsNullOrEmpty(Deductions[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Match"].ToString());
                                        ActualValue = string.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Amount"].ToString());
                                        if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                                        {
                                            var data = lstmodel.Where(a => a.FieldName == Deductions[i]["FieldName"].ToString()).Sum(a => a.FieldValue);
                                            MatchValue = string.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Amount"].ToString());
                                            ActualValue = data;
                                        }

                                        cell = new PdfPCell(new Phrase(Deductions[i]["LabelName"].ToString(), fntTableFont));
                                        cell.BorderWidth = 0;
                                        // cell.PaddingLeft = 10F;
                                        cell.PaddingLeft = 8F;
                                        DeductionTable.AddCell(cell);
                                        double MatchVal = string.IsNullOrEmpty(Deductions[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Match"].ToString());
                                        //cell = new PdfPCell(new Phrase(double.Parse(Deductions[i]["Match"].ToString()) == 0 ? " " : double.Parse(Deductions[i]["Match"].ToString()).ToString("0.00"), fntTableFont));
                                        cell = new PdfPCell(new Phrase(MatchVal == 0 ? " " : MatchVal.ToString("0.00"), fntTableFont));
                                        cell.BorderWidth = 0;
                                        cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                        DeductionTable.AddCell(cell);
                                        double val = String.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : double.Parse(Deductions[i]["Amount"].ToString());
                                        cell = new PdfPCell(new Phrase(val == 0 ? " " : val.ToString("0.00"), fntTableFont));
                                        cell.BorderWidth = 0;
                                        cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                        DeductionTable.AddCell(cell);
                                    }
                                    DedTotal += float.Parse(string.IsNullOrEmpty(ActualValue.ToString()) ? "0" : ActualValue.ToString());
                                    DedMatchTot += float.Parse(string.IsNullOrEmpty(MatchValue.ToString()) ? "0" : MatchValue.ToString());

                                    if (flag1)
                                    {


                                        MatchValue = 0;
                                        ActualValue = 0;

                                        if (!string.IsNullOrEmpty(Supress))
                                        {
                                            MatchValue = string.IsNullOrEmpty(Deductions[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Match"].ToString());
                                            ActualValue = string.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Amount"].ToString());
                                            if (dataOtherSett.MatchOrCumm == MatchOrCumm.Cumulative)
                                            {
                                                var data = lstmodel.Where(a => a.FieldName == Deductions[i]["FieldName"].ToString()).Sum(a => a.FieldValue);
                                                MatchValue = string.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Amount"].ToString());
                                                ActualValue = data;
                                            }
                                            if (MatchValue != 0 || ActualValue != 0)
                                            {
                                                if (listLoanMaster.Any(a => a.LoanCode == Deductions[i]["FieldName"].ToString()))
                                                {

                                                    cell = new PdfPCell(new Phrase(Deductions[i]["LabelName"].ToString() + " Interest", fntTableFont));
                                                }
                                                else
                                                {
                                                    cell = new PdfPCell(new Phrase(Deductions[i]["LabelName"].ToString(), fntTableFont));
                                                }
                                                cell.BorderWidth = 0;
                                                // cell.PaddingLeft = 10F;
                                                cell.PaddingLeft = 8F;
                                                DeductionTable.AddCell(cell);
                                                cell = new PdfPCell(new Phrase(MatchValue == 0 ? " " : MatchValue.ToString("0.00"), fntTableFont));
                                                cell.BorderWidth = 0;
                                                cell.PaddingRight = 20F;
                                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                                DeductionTable.AddCell(cell);

                                                if (listLoanMaster.Any(a => a.LoanCode == Deductions[i]["FieldName"].ToString()))
                                                {
                                                    string loancode = Deductions[i]["FieldName"].ToString();
                                                    var noIstallmentData = LoanInstallment.Where(a => a.EmployeeId == emp.Id && a.LoanCode == loancode && a.FCStatus != "Y").FirstOrDefault();
                                                    if (noIstallmentData != null)
                                                    {
                                                        double val = String.IsNullOrEmpty(noIstallmentData.IntersetAmt.ToString()) ? 0 : double.Parse(noIstallmentData.IntersetAmt.ToString());
                                                        cell = new PdfPCell(new Phrase(val == 0 ? " " : val.ToString("0.00"), fntTableFont));
                                                    }
                                                    else
                                                    {
                                                        cell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                    }

                                                }
                                                else
                                                {
                                                    cell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                }
                                                cell.BorderWidth = 0;
                                                cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                                DeductionTable.AddCell(cell);
                                            }
                                        }
                                        else
                                        {
                                            cell = new PdfPCell(new Phrase(Deductions[i]["LabelName"].ToString(), fntTableFont));
                                            cell.BorderWidth = 0;
                                            // cell.PaddingLeft = 10F;
                                            cell.PaddingLeft = 8F;
                                            DeductionTable.AddCell(cell);
                                            double MatchVal = string.IsNullOrEmpty(Deductions[i]["Match"].ToString()) ? 0 : Convert.ToDouble(Deductions[i]["Match"].ToString());
                                            //cell = new PdfPCell(new Phrase(double.Parse(Deductions[i]["Match"].ToString()) == 0 ? " " : double.Parse(Deductions[i]["Match"].ToString()).ToString("0.00"), fntTableFont));
                                            cell = new PdfPCell(new Phrase(MatchVal == 0 ? " " : MatchVal.ToString("0.00"), fntTableFont));
                                            cell.BorderWidth = 0;
                                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                            DeductionTable.AddCell(cell);
                                            double val = String.IsNullOrEmpty(Deductions[i]["Amount"].ToString()) ? 0 : double.Parse(Deductions[i]["Amount"].ToString());
                                            cell = new PdfPCell(new Phrase(val == 0 ? " " : val.ToString("0.00"), fntTableFont));
                                            cell.BorderWidth = 0;
                                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                            DeductionTable.AddCell(cell);
                                        }
                                    }

                                }
                            }
                            float first = 53F;
                            float[] widths = new float[] { first, 30f, 30f };
                            DeductionTable.SetWidths(widths);
                            cell = new PdfPCell();
                            cell.Colspan = 3;
                            cell.AddElement(EarningTable);
                            cell.BorderWidthBottom = 0;
                            cell.BorderWidthTop = 0;
                            cell.BorderWidthLeft = 0;
                            if (SchemaName == "Prove923458c2c1")
                            {
                                cell.BorderWidthRight = 0.5F;
                            }
                            else
                            {
                                cell.BorderWidthRight = 0;
                            }
                            MainTable.AddCell(cell);
                            cell = new PdfPCell();
                            cell.Colspan = 3;
                            cell.AddElement(DeductionTable);
                            cell.BorderWidthBottom = 0;
                            cell.BorderWidthTop = 0;
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            MainTable.AddCell(cell);
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.GrossPayString))
                            {
                                // cell = new PdfPCell(new Phrase(dataOtherSett.GrossPayString, fntTableFont));
                                cell = new PdfPCell(new Phrase(dataOtherSett.GrossPayString, fntTableFontCompBold88));
                            }
                            else
                            {
                                cell = new PdfPCell(new Phrase("TOTAL GROSS PAY", fntTableFontCompBold88));
                            }
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            cell.BorderWidthTop = 0.5F;
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            cell.PaddingLeft = 10F;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            MainTable.AddCell(cell);
                            cell = new PdfPCell(new Phrase(double.Parse(EarnMatchTot.ToString()) == 0 ? " " : double.Parse(EarnMatchTot.ToString()).ToString("0.00"), fntTableFontBold));
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            cell.BorderWidthTop = 0.5F;
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            cell.PaddingLeft = 10F;
                            cell.PaddingRight = 0;
                            MainTable.AddCell(cell);
                            double totEarVal = String.IsNullOrEmpty(EarnTotal.ToString()) ? 0 : double.Parse(EarnTotal.ToString());
                            cell = new PdfPCell(new Phrase(totEarVal == 0 ? " " : totEarVal.ToString("0.00"), fntTableFontBold));
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            cell.BorderWidthTop = 0.5F;
                            cell.BorderWidthLeft = 0;
                            if (SchemaName == "Prove923458c2c1")
                            {
                                cell.BorderWidthRight = 0.5F;
                            }
                            else
                            {
                                cell.BorderWidthRight = 0;
                            }
                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            cell.PaddingRight = 10F;
                            MainTable.AddCell(cell);
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.DedTotString))
                            {
                                // cell = new PdfPCell(new Phrase(dataOtherSett.DedTotString, fntTableFont));
                                cell = new PdfPCell(new Phrase(dataOtherSett.DedTotString, fntTableFontCompBold88));
                            }
                            else
                            {
                                cell = new PdfPCell(new Phrase("DEDUCTION TOTAL", fntTableFontCompBold88));
                            }
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            cell.BorderWidthTop = 0.5F;
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            cell.PaddingLeft = 10F;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            MainTable.AddCell(cell);
                            cell = new PdfPCell(new Phrase(double.Parse(DedMatchTot.ToString()) == 0 ? " " : double.Parse(DedMatchTot.ToString()).ToString("0.00"), fntTableFontBold));
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            cell.BorderWidthTop = 0.5F;
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            MainTable.AddCell(cell);
                            double totDedVal = String.IsNullOrEmpty(DedTotal.ToString()) ? 0 : double.Parse(DedTotal.ToString());
                            cell = new PdfPCell(new Phrase(DedTotal == 0 ? " " : DedTotal.ToString("0.00"), fntTableFontBold));
                            cell.BackgroundColor = BaseColor.WHITE;
                            cell.BorderColor = BaseColor.BLACK;
                            cell.BorderWidthBottom = 0.5F;
                            cell.BorderWidthTop = 0.5F;
                            cell.BorderWidthLeft = 0;
                            cell.BorderWidthRight = 0;
                            cell.HorizontalAlignment = Element.ALIGN_RIGHT;
                            cell.PaddingTop = 3F;
                            cell.PaddingBottom = 3F;
                            MainTable.AddCell(cell);
                            #endregion
                            #endregion
                            #region Net Pay & In RUPEES
                            //// Empty Row
                            //cellEmpty = new PdfPCell(new Phrase(" ", fntTableFont));
                            //cellEmpty.Colspan = 6;
                            //cellEmpty.BorderWidth = 0;
                            //cellEmpty.BorderWidth = 0;
                            //cellEmpty.FixedHeight = 5f;
                            //MainTable.AddCell(cellEmpty);
                            int Yearint = int.Parse(year);
                            //var netpay = empTranscData.Where(c => c.CompanyId == companyId && c.EmployeeId == emp.Id && c.Month == month && c.Year == Yearint).Select(c => c.NetPay).FirstOrDefault();
                            var netpay = empTranscData.Where(c => c.EmployeeId == emp.Id).Select(c => c.NetPay).FirstOrDefault();
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.NetSalString))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.NetSalString, fntTableFontCompBold88));
                            }
                            else
                            {
                                cellHead = new PdfPCell(new Phrase("NET PAY : ", fntTableFontCompBold88));
                            }
                            if (YTDFlag)
                            {
                                cellHead.Colspan = 4;
                            }
                            else { cellHead.Colspan = 2; }
                            cellHead.BorderWidth = 0;
                            cellHead.BorderWidthBottom = 0;
                            cellHead.BorderWidthTop = 0;
                            cellHead.BorderWidthLeft = 0;
                            cellHead.BorderWidthRight = 0;
                            cellHead.PaddingLeft = 10F;
                            MainTable.AddCell(cellHead);
                            cellHead = new PdfPCell(new Phrase(netpay.ToString("0.00"), fntTableFontBold));
                            cellHead.Colspan = 1;
                            cellHead.BorderWidth = 0;
                            cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                            cellHead.BorderWidthBottom = 0;
                            cellHead.BorderWidthTop = 0;
                            cellHead.BorderWidthLeft = 0;
                            cellHead.BorderWidthRight = 0;
                            // Ranjit 18/02/2019 Smollan Changes
                            if (YTDFlag)
                            {
                                cellHead.PaddingRight = 2F;
                            }
                            else { cellHead.PaddingRight = 10F; }
                            MainTable.AddCell(cellHead);
                            // Ranjit Addedd Smollan Changes
                            double YTDCalAmt = EarnTotal - DedTotal;
                            if (YTDFlag && dataOtherSett.YTDCalculation)
                            {
                                cellHead = new PdfPCell(new Phrase(YTDCalAmt.ToString("0.00"), fntTableFontBold));
                            }
                            else
                            {
                                cellHead = new PdfPCell(new Phrase("", fntTableFont));
                            }
                            cellHead.Colspan = 3;
                            cellHead.BorderWidth = 0;
                            // Ranjit Addedd Smollan Changes
                            if (YTDFlag)
                            {
                                cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                            }
                            else { cellHead.HorizontalAlignment = Element.ALIGN_LEFT; }
                            cellHead.BorderWidthBottom = 0;
                            cellHead.BorderWidthTop = 0;
                            cellHead.BorderWidthLeft = 0;
                            cellHead.BorderWidthRight = 0;
                            MainTable.AddCell(cellHead);
                            string[] npay = Math.Round(netpay, 2).ToString().Split('.');
                            var currencyType = "RUPEES";
                            if (dataOtherSett.CurrencyType != 0)
                            {
                                currencyType = Util.GetEnumDescription(dataOtherSett.CurrencyType);
                                if (currencyType == "RUPEES") { currencyType = "Rupees"; }
                            }
                            if (int.Parse(npay[0]) != 0)
                            {
                                if (SchemaName != "Bigtaa66c09f1e4")
                                {
                                    if (SchemaName == "morph1f03422b1d" && companyId == 4)
                                    {
                                        cellHead = new PdfPCell(new Phrase("(DIRHAMS " + Util.NumbersToWords(int.Parse(npay[0])) + " Only)", fntTableFontCompBold88));
                                    }
                                    else if (SchemaName == "Terraadffc1142d")
                                    {
                                        cellHead = new PdfPCell(new Phrase("(" + Util.NumbersToWords(int.Parse(npay[0])) + " Only)", fntTableFontCompBold88));
                                    }
                                    else
                                    {
                                        cellHead = new PdfPCell(new Phrase("(" + currencyType + " " + Util.NumbersToWords(int.Parse(npay[0])) + " Only)", fntTableFontCompBold88));
                                    }
                                }
                                else if (companyId == 2)
                                {
                                    cellHead = new PdfPCell(new Phrase("(SGD " + Util.NumbersToWords(int.Parse(npay[0])) + " Only)", fntTableFont));
                                }
                                else
                                {
                                    cellHead = new PdfPCell(new Phrase("(" + currencyType + " " + Util.NumbersToWords(int.Parse(npay[0])) + " Only)", fntTableFontCompBold88));
                                }
                                cellHead.Colspan = 6;
                                cellHead.BorderWidthTop = 0.5F;
                                cellHead.BorderWidth = 0;
                                cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                MainTable.AddCell(cellHead);
                            }
                            #endregion
                            #region Printing fields in footer section & Signature - Box 4
                            // ------------ Footer Section - Starts
                            var FooterFields = lstHeaderFields.Where(p => p.PayConNo == PayConfigNo && p.FooterPrintOrder != 0 && p.CompanyId == companyId).OrderBy(p => p.FooterPrintOrder).ToList();
                            int tempcounterFooter = 0;
                            foreach (var masterfields in FooterFields)
                            {
                                if (masterfields.ComponentType == ComponentType.ManualFields)
                                {
                                    var masterFieldVal = _employeeRepository.GetEmpDataFieldWise(SchemaName, companyId, emp.Id, masterfields.FieldName, masterfields.LabelName, emp, dataComp, lstPayConfigNo, empConfig, dataCombo, customData);
                                    cellHead = new PdfPCell(new Phrase(masterfields.LabelName + " : ", fntTableFont));
                                    cellHead.Colspan = 2;
                                    cellHead.BorderWidth = 0;
                                    cellHead.PaddingLeft = 10F;
                                    MainTable.AddCell(cellHead);
                                    DateTime tempDate = new DateTime();
                                    if (masterFieldVal != null)
                                    {
                                        if (DateTime.TryParse(masterFieldVal.ToString(), out tempDate))
                                        {
                                            cellHead = new PdfPCell(new Phrase(String.Format("{0:dd/MM/yyyy}", tempDate), fntTableFont));
                                        }
                                        else
                                        {
                                            cellHead = new PdfPCell(new Phrase(masterFieldVal.ToString(), fntTableFont));
                                        }
                                    }
                                    else
                                    {
                                        cellHead = new PdfPCell(new Phrase(masterFieldVal, fntTableFont));
                                    }
                                    cellHead.BorderWidth = 0;
                                    cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                                    cellHead.PaddingRight = 10F;
                                    MainTable.AddCell(cellHead);
                                    tempcounterFooter++;
                                }
                                else
                                {
                                    DataRow[] dr = datatable.Select(" FieldName = '" + masterfields.FieldName + "'");
                                    double value = string.IsNullOrEmpty(dr[0]["Amount"].ToString()) ? 0 : double.Parse(dr[0]["Amount"].ToString());

                                    if (!string.IsNullOrEmpty(Supress))
                                    {

                                        if (value != 0)
                                        {
                                            cellHead = new PdfPCell(new Phrase(dr[0]["LabelName"] + " : ", fntTableFont));
                                            cellHead.BorderWidth = 0;
                                            cellHead.Colspan = 2;
                                            cellHead.PaddingLeft = 10F;
                                            MainTable.AddCell(cellHead);
                                            cellHead = new PdfPCell(new Phrase(value.ToString("0.00"), fntTableFont));
                                            cellHead.BorderWidth = 0;
                                            cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                                            cellHead.PaddingRight = 10F;
                                            MainTable.AddCell(cellHead);
                                            tempcounterFooter++;
                                        }
                                    }
                                    else
                                    {
                                        cellHead = new PdfPCell(new Phrase(dr[0]["LabelName"] + " : ", fntTableFont));
                                        cellHead.BorderWidth = 0;
                                        cellHead.Colspan = 2;
                                        cellHead.PaddingLeft = 10F;
                                        MainTable.AddCell(cellHead);
                                        // cellHead = new PdfPCell(new Phrase((double.Parse(amount) == 0 ? " " : double.Parse(amount).ToString("0.00")), fntTableFont));
                                        cellHead = new PdfPCell(new Phrase(value.ToString("0.00"), fntTableFont));
                                        cellHead.BorderWidth = 0;
                                        cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                                        cellHead.PaddingRight = 10F;
                                        MainTable.AddCell(cellHead);
                                        tempcounterFooter++;
                                    }
                                }
                            }
                            if ((tempcounterFooter % 2) != 0)
                            {
                                cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                cellHead.Colspan = 3;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                            }
                            int tempCounter = 0;
                            if (dataOtherSett != null && dataOtherSett.BL != false)
                            {
                                string leaveOpenBalance = "0";
                                if (leaveFinNo != 0)
                                {
                                    var dateFin = new DateTime(int.Parse(year), (int)month, 1);
                                    var finModal = leaveFinAll.Where(a => dateFin.Date >= a.FinStart.Date && dateFin.Date <= a.FinEnd.Date).FirstOrDefault();

                                    //var finmod= from prod in leaveFinAll Where(prod.FinStart >= dateFin && prod.DateAdded <= endDate)
                                    if (finModal != null)
                                    {
                                        var ReqiureLeave = ctx.LeaveMaster.Where(a => a.CompanyId == companyId && a.FinNo == finModal.FinNo && a.ShowInPaySlip == true && a.Op_Required == true).Select(a => a.Leave).ToList();
                                        var data = ctx.LeaveOpening.Where(a => a.CompanyId == companyId && a.EmployeeId == emp.Id && a.FinNo == finModal.FinNo && ReqiureLeave.Contains(a.Leave)).Select(a => new { a.LeaveOpen, a.LeaveUsed, a.LeaveCr, a.LeaveDb }).ToList();
                                        var temp1 = data.Sum(a => a.LeaveOpen) + data.Sum(a => a.LeaveCr);
                                        var temp2 = data.Sum(a => a.LeaveUsed) + data.Sum(a => a.LeaveDb);
                                        leaveOpenBalance = (temp1 - temp2).ToString("0.00");
                                    }
                                    else
                                    {
                                        leaveOpenBalance = "0";
                                    }
                                }
                                else
                                {
                                    leaveOpenBalance = "0";
                                }
                                cell = new PdfPCell(new Phrase("Leave Balance : " + leaveOpenBalance, fntTableFont));
                                cell.Colspan = 3;
                                cell.BorderWidth = 0;
                                MainTable.AddCell(cell);
                                tempCounter++;
                            }
                            if (dataOtherSett != null && dataOtherSett.LT != false)
                            {
                                string leaveUsedBalance = "0";
                                if (leaveFinNo != 0)
                                {
                                    DateTime toDate = new DateTime(int.Parse(Year), int.Parse(Month), 1).AddMonths(1);
                                    var leaveUsedData = ctx.LeaveAbsent.Where(a => a.CompanyId == companyId && a.EmployeeId == emp.Id && a.LeaveDate >= PayDate && a.LeaveDate < toDate).Any() ? ctx.LeaveAbsent.Where(a => a.CompanyId == companyId && a.EmployeeId == emp.Id && a.LeaveDate >= PayDate && a.LeaveDate < toDate).Sum(a => a.HFDay) : 0;
                                    //var data = ctx.LeaveOpening.Where(a => a.CompanyId == companyId && a.EmployeeId == emp.Id && a.FinNo == leaveFinNo).Select(a => new { a.LeaveUsed }).ToList();
                                    leaveUsedBalance = leaveUsedData.ToString();
                                }
                                else
                                {
                                    leaveUsedBalance = "0";
                                }
                                cell = new PdfPCell(new Phrase("Leave Taken : " + leaveUsedBalance, fntTableFont));
                                cell.Colspan = 3;
                                cell.BorderWidth = 0;
                                MainTable.AddCell(cell);
                                tempCounter++;
                            }
                            if (dataOtherSett != null && dataOtherSett.LD != false)
                            {
                                string Query = "declare @Date nvarchar(max), @Company  nvarchar(max),@Employee nvarchar(max)  set @Date = '_Date'  set @Company = '_Company' set @Employee = '_Employee'  exec('select LoanCode,SUM(AmtPerMonth) as balance  from dbo.MonthLoan  where MonthlyDate > '+ @Date +' and LoanDate <=  '+ @Date +'  and CompanyId = '+ @Company +' and LoanCode in (select distinct LoanCode from dbo.LoanEntry  where  LoanDate <=  '+ @Date +'  and EmployeeId = '+ @Employee +' and CompanyId = '+ @Company +')  and EmployeeId = '+ @Employee +' and (FCStatus  <> ''Y'' or FCStatus is null)  group by LoanCode')  ";
                                Query = Query.Replace("dbo", SchemaName);
                                Query = Query.Replace("_Company", companyId.ToString());
                                Query = Query.Replace("_Employee", emp.Id.ToString());
                                Query = Query.Replace("_Date", "convert(datetime,''" + new DateTime(int.Parse(Year), int.Parse(Month), DateTime.DaysInMonth(int.Parse(Year), int.Parse(Month))).ToString("dd/MM/yyyy", CultureInfo.InvariantCulture) + "'',103)");
                                var dtLoanDetail = new DataTable();
                                dtLoanDetail = dBManager.GetDataTable(Query, CommandType.Text);
                                if (dtLoanDetail.Rows.Count > 0)
                                {
                                    string LoanTaken = string.Empty;
                                    for (int i = 0; i < dtLoanDetail.Rows.Count; i++)
                                    {
                                        LoanTaken += dtLoanDetail.Rows[i]["LoanCode"].ToString() + " = " + dtLoanDetail.Rows[i]["balance"].ToString() + ",";
                                    }
                                    if (LoanTaken != string.Empty)
                                    {
                                        LoanTaken = LoanTaken.Substring(0, LoanTaken.Length - 1);
                                    }
                                    cell = new PdfPCell(new Phrase("Loan Details : " + LoanTaken, fntTableFont));
                                }
                                else
                                {
                                    cell = new PdfPCell(new Phrase("Loan Details : ", fntTableFont));
                                }
                                cell.Colspan = 3;
                                cell.BorderWidth = 0;
                                MainTable.AddCell(cell);
                                tempCounter++;
                            }
                            // First String
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.FirstString))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.FirstString, fntTableFont));
                                cellHead.Colspan = 3;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                                tempCounter++;
                            }
                            // Second String
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.SecondString))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.SecondString, fntTableFont));
                                cellHead.Colspan = 3;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                                tempCounter++;
                            }
                            // Third String
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.ThirdString))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.ThirdString, fntTableFont));
                                cellHead.Colspan = 3;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                                tempCounter++;
                            }
                            if ((tempCounter % 2) != 0)
                            {
                                cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                cellHead.Colspan = 3;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                            }
                            //GenericLogic:
                            //if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Footer1))
                            //=======
                            // Footer Strings
                            // Customized

                            if (!string.IsNullOrEmpty(Leave))
                            {
                                if (displayLeaves.Count > 0)
                                {
                                    PdfPTable LeaveTable = new PdfPTable(displayLeaves.Count + 1);
                                    LeaveTable.WidthPercentage = 100;
                                    PdfPCell LeaveHead = new PdfPCell(new Phrase("Leave Details", fntTableFontBold));
                                    LeaveHead.Colspan = displayLeaves.Count + 1;
                                    LeaveHead.PaddingTop = 3F;
                                    LeaveHead.PaddingBottom = 3F;
                                    LeaveHead.HorizontalAlignment = Element.ALIGN_LEFT;
                                    LeaveTable.AddCell(LeaveHead);
                                    PdfPCell Leavecell = new PdfPCell(new Phrase("Leave Type", fntTableFontBold));
                                    LeaveTable.AddCell(Leavecell);
                                    if (displayLeaves.Contains("opening"))
                                    {
                                        Leavecell = new PdfPCell(new Phrase("Opening", fntTableFontBold));
                                        LeaveTable.AddCell(Leavecell);
                                    }
                                    if (displayLeaves.Contains("credit"))
                                    {
                                        Leavecell = new PdfPCell(new Phrase("Credit", fntTableFontBold));
                                        LeaveTable.AddCell(Leavecell);
                                    }
                                    if (displayLeaves.Contains("used"))
                                    {
                                        Leavecell = new PdfPCell(new Phrase("Used", fntTableFontBold));
                                        LeaveTable.AddCell(Leavecell);
                                    }
                                    if (displayLeaves.Contains("debit"))
                                    {
                                        Leavecell = new PdfPCell(new Phrase("Debit", fntTableFontBold));
                                        LeaveTable.AddCell(Leavecell);
                                    }
                                    if (displayLeaves.Contains("balance"))
                                    {
                                        Leavecell = new PdfPCell(new Phrase("Balance", fntTableFontBold));
                                        LeaveTable.AddCell(Leavecell);
                                    }

                                    if (empLeaveDetails.Count > 0)
                                    {

                                        foreach (var item in lstLeaveMaster)
                                        {
                                            var leaveData = empLeaveDetails.Where(m => m.Leave == item.Leave && m.EmployeeId == emp.Id).FirstOrDefault();
                                            //var usedLeave = leaveAbsents.Where(m => m.Leave == item.Leave && m.EmployeeId == emp.Id && MonthList.Contains((InputMonths)m.LeaveDate.Month) && yearList.Contains(m.LeaveDate.Year)).Sum(a => a.HFDay);
                                            var usedLeave = leaveAbsents.Where(m => m.Leave == item.Leave && m.EmployeeId == emp.Id && m.LeaveDate.Month != (int)myQueItem.month).Sum(a => a.HFDay);
                                            double creditLeave = 0;
                                            if (leaveSet != null)
                                            {
                                                creditLeave = LeaveCreditMonthly.Where(a => a.Leave == item.Leave && a.EmployeeId == emp.Id && (a.InputYear < myQueItem.year || (a.InputYear == myQueItem.year && (int)a.InputMonth < ((int)myQueItem.month)))).Sum(a => a.LeaveCr);               //Select(a => a.LeaveCr).FirstOrDefault();
                                            }
                                            else
                                            {
                                                creditLeave = empLeaveDetails.Where(a => a.Leave == item.Leave && a.EmployeeId == emp.Id).Select(a => a.LeaveCr).FirstOrDefault();
                                            }
                                            var debitLeave = selfServiceDebitLeave.Where(a => a.LeaveType == item.Leave && a.EmployeeId == emp.Id && a.LeaveDate.Month != (int)myQueItem.month).Sum(a => a.NoOfDays);
                                            var currentDebitLeave = selfServiceDebitLeave.Where(a => a.LeaveType == item.Leave && a.EmployeeId == emp.Id && a.LeaveDate.Month == (int)myQueItem.month && a.LeaveDate.Year == (int)myQueItem.year).Sum(a => a.NoOfDays);
                                            var currentCreditLeave = LeaveCreditMonthly.Where(a => a.Leave == item.Leave && a.EmployeeId == emp.Id && a.InputMonth == (InputMonths)myQueItem.month && a.InputYear == (int)myQueItem.year).Select(a => a.LeaveCr).FirstOrDefault();
                                            var currentUsedLeave = leaveAbsents.Where(m => m.Leave == item.Leave && m.EmployeeId == emp.Id && m.LeaveDate.Month == (int)myQueItem.month && m.LeaveDate.Year == (int)myQueItem.year).Sum(a => a.HFDay);
                                            double leaveOpen = 0;
                                            if (leaveData == null)
                                            {
                                                leaveData = new LeaveOpening();
                                                leaveData.LeaveUsed = leaveAbsents.Where(a => a.Leave == item.Leave && a.EmployeeId == emp.Id).Sum(a => a.HFDay);
                                            }
                                            string LeaveDesc = string.IsNullOrEmpty(item.LeaveDesc) ? item.Leave : item.LeaveDesc;
                                            Leavecell = new PdfPCell(new Phrase(LeaveDesc, fntTableFont));
                                            LeaveTable.AddCell(Leavecell);
                                            if (displayLeaves.Contains("opening"))
                                            {
                                                if (item.Op_Required)
                                                {
                                                    if (finstartDate.Month == (int)myQueItem.month && finstartDate.Year == (int)myQueItem.year)
                                                    {
                                                        leaveOpen = leaveData.LeaveOpen;
                                                        Leavecell = new PdfPCell(new Phrase(leaveData.LeaveOpen.ToString("0.00"), fntTableFont));
                                                        LeaveTable.AddCell(Leavecell);
                                                    }
                                                    else
                                                    {
                                                        leaveOpen = (leaveData.LeaveOpen + creditLeave) - usedLeave - debitLeave;

                                                        Leavecell = new PdfPCell(new Phrase(leaveOpen.ToString("0.00"), fntTableFont));
                                                        LeaveTable.AddCell(Leavecell);
                                                    }
                                                }
                                                else
                                                {
                                                    Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                    LeaveTable.AddCell(Leavecell);
                                                }
                                            }
                                            if (displayLeaves.Contains("credit"))
                                            {
                                                //Leavecell = new PdfPCell(new Phrase(leaveData.LeaveCr.ToString("0.00"), fntTableFont));
                                                Leavecell = new PdfPCell(new Phrase(currentCreditLeave.ToString("0.00"), fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }
                                            if (displayLeaves.Contains("used"))
                                            {

                                                //Leavecell = new PdfPCell(new Phrase(leaveData.LeaveUsed.ToString("0.00"), fntTableFont));
                                                Leavecell = new PdfPCell(new Phrase(currentUsedLeave.ToString("0.00"), fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }
                                            if (displayLeaves.Contains("debit"))
                                            {
                                                //Leavecell = new PdfPCell(new Phrase(leaveData.LeaveDb.ToString("0.00"), fntTableFont));
                                                Leavecell = new PdfPCell(new Phrase(currentDebitLeave.ToString("0.00"), fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }
                                            if (displayLeaves.Contains("balance"))
                                            {
                                                if (item.Op_Required)
                                                    //Leavecell = new PdfPCell(new Phrase(((leaveOpen + currentCreditLeave) - currentUsedLeave - currentDebitLeave).ToString("0.00"), fntTableFont));
                                                    Leavecell = new PdfPCell(new Phrase(((leaveOpen + currentCreditLeave) - currentUsedLeave - currentDebitLeave).ToString("0.00"), fntTableFont));
                                                else
                                                    Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));

                                                LeaveTable.AddCell(Leavecell);
                                            }

                                        }
                                        //foreach (var item in empLeaveDetails.Where(m => m.EmployeeId == emp.Id))
                                        //{
                                        //    //Leavecell = new PdfPCell(new Phrase(item.Leave, fntTableFont)); // Add ram  LeaveDesc
                                        //    string LeaveDesc = string.IsNullOrEmpty(lstLeaveMaster.Where(a => a.Leave == item.Leave).FirstOrDefault().LeaveDesc) ? item.Leave : lstLeaveMaster.Where(a => a.Leave == item.Leave).FirstOrDefault().LeaveDesc;
                                        //    Leavecell = new PdfPCell(new Phrase(LeaveDesc, fntTableFont));
                                        //    LeaveTable.AddCell(Leavecell);
                                        //    if (displayLeaves.Contains("opening"))
                                        //    {
                                        //        Leavecell = new PdfPCell(new Phrase(item.LeaveOpen.ToString("0.00"), fntTableFont));
                                        //        LeaveTable.AddCell(Leavecell);
                                        //    }
                                        //    if (displayLeaves.Contains("credit"))
                                        //    {
                                        //        Leavecell = new PdfPCell(new Phrase(item.LeaveCr.ToString("0.00"), fntTableFont));
                                        //        LeaveTable.AddCell(Leavecell);
                                        //    }
                                        //    if (displayLeaves.Contains("used"))
                                        //    {
                                        //        Leavecell = new PdfPCell(new Phrase(item.LeaveUsed.ToString("0.00"), fntTableFont));
                                        //        LeaveTable.AddCell(Leavecell);
                                        //    }
                                        //    if (displayLeaves.Contains("balance"))
                                        //    {
                                        //        Leavecell = new PdfPCell(new Phrase(((item.LeaveOpen + item.LeaveCr) - item.LeaveUsed - item.LeaveDb).ToString("0.00"), fntTableFont));
                                        //        LeaveTable.AddCell(Leavecell);
                                        //    }
                                        //}
                                    }
                                    else
                                    {
                                        foreach (var item in lstLeaveMaster)
                                        {
                                            // Leavecell = new PdfPCell(new Phrase(item.Leave, fntTableFont));
                                            // Add Ram
                                            Leavecell = new PdfPCell(new Phrase(item.LeaveDesc, fntTableFont));
                                            LeaveTable.AddCell(Leavecell);
                                            if (displayLeaves.Contains("opening"))
                                            {
                                                Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }
                                            if (displayLeaves.Contains("credit"))
                                            {
                                                Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }
                                            if (displayLeaves.Contains("used"))
                                            {
                                                Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }
                                            if (displayLeaves.Contains("debit"))
                                            {
                                                Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }
                                            if (displayLeaves.Contains("balance"))
                                            {
                                                Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                                                LeaveTable.AddCell(Leavecell);
                                            }

                                        }
                                    }
                                    cellHead = new PdfPCell();
                                    cellHead.AddElement(LeaveTable);
                                    cellHead.Colspan = 6;
                                    cellHead.BorderWidth = 0;
                                    MainTable.AddCell(cellHead);
                                }
                            }

                            try
                            {
                                if (!String.IsNullOrEmpty(dataOtherSett.Footer1) && dataOtherSett.Footer1.Trim().StartsWith("#") && dataOtherSett.Footer1.Trim().EndsWith("#") && !String.IsNullOrEmpty(dataOtherSett.Footer6) && dataOtherSett.Footer6.Trim().StartsWith("#") && dataOtherSett.Footer6.Trim().EndsWith("#"))
                                {
                                    cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer1.Replace(@"#", string.Empty), fntTableFont));
                                    cellHead.Colspan = 3;
                                    cellHead.BorderWidth = 0;
                                    cellHead.HorizontalAlignment = Element.ALIGN_LEFT;
                                    MainTable.AddCell(cellHead);
                                    cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer6.Replace(@"#", string.Empty), fntTableFont));
                                    cellHead.Colspan = 3;
                                    cellHead.BorderWidth = 0;
                                    cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                                    MainTable.AddCell(cellHead);
                                    goto SkipGenericLogic;
                                }
                            }
                            catch (Exception)
                            {
                                goto GenericLogic;
                            }
                        GenericLogic:
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Footer1))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer1, fntTableFont));
                                cellHead.Colspan = 6;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                            }
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Footer2))
                            {
                                if (dataOtherSett.Footer2 != " ")
                                {
                                    cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer2, fntTableFont));
                                    cellHead.Colspan = 6;
                                    cellHead.BorderWidth = 0;
                                    MainTable.AddCell(cellHead);
                                }
                            }
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Footer3))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer3, fntTableFont));
                                cellHead.Colspan = 6;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                            }
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Footer4))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer4, fntTableFont));
                                cellHead.Colspan = 6;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                            }
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Footer5))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer5, fntTableFont));
                                cellHead.Colspan = 6;
                                cellHead.BorderWidth = 0;
                                MainTable.AddCell(cellHead);
                            }
                            if (dataOtherSett != null && !string.IsNullOrEmpty(dataOtherSett.Footer6))
                            {
                                cellHead = new PdfPCell(new Phrase(dataOtherSett.Footer6, fntTableFont));
                                cellHead.Colspan = 6;
                                cellHead.BorderWidth = 0;
                                cell.PaddingRight = 5F;
                                cell.PaddingBottom = 1F;
                                cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                                MainTable.AddCell(cellHead);
                            }
                        // ------------ Footer Section - Ends
                        SkipGenericLogic:
                            #region Leave Table
                            //// Empty Row
                            //cellEmpty = new PdfPCell(new Phrase(" ", fntTableFont));
                            //cellEmpty.Colspan = 6;
                            //cellEmpty.BorderWidth = 0;
                            //cellEmpty.BorderWidth = 0;
                            //cellEmpty.FixedHeight = 15f;
                            //MainTable.AddCell(cellEmpty);
                            //if (!string.IsNullOrEmpty(Leave))
                            //{
                            //    if (displayLeaves.Count > 0)
                            //    {
                            //        PdfPTable LeaveTable = new PdfPTable(displayLeaves.Count + 1);
                            //        LeaveTable.WidthPercentage = 100;
                            //        PdfPCell LeaveHead = new PdfPCell(new Phrase("Leave Details", fntTableFontBold));
                            //        LeaveHead.Colspan = displayLeaves.Count + 1;
                            //        LeaveHead.PaddingTop = 3F;
                            //        LeaveHead.PaddingBottom = 3F;
                            //        LeaveHead.HorizontalAlignment = Element.ALIGN_LEFT;
                            //        LeaveTable.AddCell(LeaveHead);
                            //        PdfPCell Leavecell = new PdfPCell(new Phrase("Leave Type", fntTableFontBold));
                            //        LeaveTable.AddCell(Leavecell);
                            //        if (displayLeaves.Contains("opening"))
                            //        {
                            //            Leavecell = new PdfPCell(new Phrase("Opening", fntTableFontBold));
                            //            LeaveTable.AddCell(Leavecell);
                            //        }
                            //        if (displayLeaves.Contains("credit"))
                            //        {
                            //            Leavecell = new PdfPCell(new Phrase("Credit", fntTableFontBold));
                            //            LeaveTable.AddCell(Leavecell);
                            //        }
                            //        if (displayLeaves.Contains("used"))
                            //        {
                            //            Leavecell = new PdfPCell(new Phrase("Used", fntTableFontBold));
                            //            LeaveTable.AddCell(Leavecell);
                            //        }
                            //        if (displayLeaves.Contains("debit"))
                            //        {
                            //            Leavecell = new PdfPCell(new Phrase("Debit", fntTableFontBold));
                            //            LeaveTable.AddCell(Leavecell);
                            //        }
                            //        if (displayLeaves.Contains("balance"))
                            //        {
                            //            Leavecell = new PdfPCell(new Phrase("Balance", fntTableFontBold));
                            //            LeaveTable.AddCell(Leavecell);
                            //        }

                            //        if (empLeaveDetails.Count > 0)
                            //        {

                            //            foreach (var item in lstLeaveMaster)
                            //            {
                            //                var leaveData = empLeaveDetails.Where(m => m.Leave == item.Leave && m.EmployeeId == emp.Id).FirstOrDefault() ?? new LeaveOpening();
                            //                string LeaveDesc = string.IsNullOrEmpty(item.LeaveDesc) ? item.Leave : item.LeaveDesc;
                            //                Leavecell = new PdfPCell(new Phrase(LeaveDesc, fntTableFont));
                            //                LeaveTable.AddCell(Leavecell);
                            //                if (displayLeaves.Contains("opening"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase(leaveData.LeaveOpen.ToString("0.00"), fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("credit"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase(leaveData.LeaveCr.ToString("0.00"), fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("used"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase(leaveData.LeaveUsed.ToString("0.00"), fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("debit"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase(leaveData.LeaveDb.ToString("0.00"), fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("balance"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase(((leaveData.LeaveOpen + leaveData.LeaveCr) - leaveData.LeaveUsed - leaveData.LeaveDb).ToString("0.00"), fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }

                            //            }
                            //            //foreach (var item in empLeaveDetails.Where(m => m.EmployeeId == emp.Id))
                            //            //{
                            //            //    //Leavecell = new PdfPCell(new Phrase(item.Leave, fntTableFont)); // Add ram  LeaveDesc
                            //            //    string LeaveDesc = string.IsNullOrEmpty(lstLeaveMaster.Where(a => a.Leave == item.Leave).FirstOrDefault().LeaveDesc) ? item.Leave : lstLeaveMaster.Where(a => a.Leave == item.Leave).FirstOrDefault().LeaveDesc;
                            //            //    Leavecell = new PdfPCell(new Phrase(LeaveDesc, fntTableFont));
                            //            //    LeaveTable.AddCell(Leavecell);
                            //            //    if (displayLeaves.Contains("opening"))
                            //            //    {
                            //            //        Leavecell = new PdfPCell(new Phrase(item.LeaveOpen.ToString("0.00"), fntTableFont));
                            //            //        LeaveTable.AddCell(Leavecell);
                            //            //    }
                            //            //    if (displayLeaves.Contains("credit"))
                            //            //    {
                            //            //        Leavecell = new PdfPCell(new Phrase(item.LeaveCr.ToString("0.00"), fntTableFont));
                            //            //        LeaveTable.AddCell(Leavecell);
                            //            //    }
                            //            //    if (displayLeaves.Contains("used"))
                            //            //    {
                            //            //        Leavecell = new PdfPCell(new Phrase(item.LeaveUsed.ToString("0.00"), fntTableFont));
                            //            //        LeaveTable.AddCell(Leavecell);
                            //            //    }
                            //            //    if (displayLeaves.Contains("balance"))
                            //            //    {
                            //            //        Leavecell = new PdfPCell(new Phrase(((item.LeaveOpen + item.LeaveCr) - item.LeaveUsed - item.LeaveDb).ToString("0.00"), fntTableFont));
                            //            //        LeaveTable.AddCell(Leavecell);
                            //            //    }
                            //            //}
                            //        }
                            //        else
                            //        {
                            //            foreach (var item in lstLeaveMaster)
                            //            {
                            //                // Leavecell = new PdfPCell(new Phrase(item.Leave, fntTableFont));
                            //                // Add Ram
                            //                Leavecell = new PdfPCell(new Phrase(item.LeaveDesc, fntTableFont));
                            //                LeaveTable.AddCell(Leavecell);
                            //                if (displayLeaves.Contains("opening"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("credit"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("used"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("debit"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }
                            //                if (displayLeaves.Contains("balance"))
                            //                {
                            //                    Leavecell = new PdfPCell(new Phrase("0.00", fntTableFont));
                            //                    LeaveTable.AddCell(Leavecell);
                            //                }

                            //            }
                            //        }
                            //        cellHead = new PdfPCell();
                            //        cellHead.AddElement(LeaveTable);
                            //        cellHead.Colspan = 6;
                            //        cellHead.BorderWidth = 0;
                            //        MainTable.AddCell(cellHead);
                            //    }
                            //}
                            #endregion
                            #region Signature
                            if (payDefaultSett.Any() && payDefaultSett.Any(m => m.FieldValue2 == "SIGNATURE_LEFT_URL" || m.FieldValue2 == "SIGNATURE_CENTER_URL" || m.FieldValue2 == "SIGNATURE_RIGHT_URL"))
                            {
                                string signUrl = "", signText1 = "", signText2 = "", signText3 = "";
                                #region Left
                                signUrl = payDefaultSett.Where(m => m.FieldValue2 == "SIGNATURE_LEFT_URL").Select(m => m.FieldValue3).FirstOrDefault();
                                signText1 = payDefaultSett.Where(m => m.FieldValue2 == "SIGNATURE_LEFT_TEXT").Select(m => m.FieldValue3).FirstOrDefault();
                                if (!String.IsNullOrEmpty(signUrl))
                                {
                                    try
                                    {
                                        iTextSharp.text.Image jpg = iTextSharp.text.Image.GetInstance(signUrl);
                                        PdfPCell signCell = new PdfPCell(jpg);
                                        jpg.ScaleAbsolute(100F, 25F);
                                        signCell.Colspan = 2;
                                        signCell.BorderWidth = 0;
                                        signCell.PaddingRight = 5F;
                                        signCell.PaddingBottom = 0.5F;
                                        signCell.HorizontalAlignment = Element.ALIGN_CENTER;
                                        MainTable.AddCell(signCell);
                                    }
                                    catch (Exception)
                                    {
                                        cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                        cellHead.Colspan = 2;
                                        cellHead.BorderWidth = 0;
                                        cell.PaddingRight = 5F;
                                        cell.PaddingBottom = 0.5F;
                                        cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                        MainTable.AddCell(cellHead);
                                    }
                                }
                                else
                                {
                                    cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                    cellHead.Colspan = 2;
                                    cellHead.BorderWidth = 0;
                                    cell.PaddingRight = 5F;
                                    cell.PaddingBottom = 0.5F;
                                    cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                    MainTable.AddCell(cellHead);
                                }
                                #endregion
                                #region Center
                                signUrl = payDefaultSett.Where(m => m.FieldValue2 == "SIGNATURE_CENTER_URL").Select(m => m.FieldValue3).FirstOrDefault();
                                signText2 = payDefaultSett.Where(m => m.FieldValue2 == "SIGNATURE_CENTER_TEXT").Select(m => m.FieldValue3).FirstOrDefault();
                                if (!String.IsNullOrEmpty(signUrl))
                                {
                                    try
                                    {
                                        iTextSharp.text.Image jpg = iTextSharp.text.Image.GetInstance(signUrl);
                                        PdfPCell signCell = new PdfPCell(jpg);
                                        if (SchemaName != "SWORDd8a826182c")
                                        {
                                            jpg.ScaleAbsolute(100F, 25F);
                                        }
                                        signCell.Colspan = 2;
                                        signCell.BorderWidth = 0;
                                        signCell.PaddingRight = 5F;
                                        signCell.PaddingBottom = 0.5F;
                                        signCell.HorizontalAlignment = Element.ALIGN_CENTER;
                                        MainTable.AddCell(signCell);
                                    }
                                    catch (Exception)
                                    {
                                        cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                        cellHead.Colspan = 2;
                                        cellHead.BorderWidth = 0;
                                        cell.PaddingRight = 5F;
                                        cell.PaddingBottom = 0.5F;
                                        cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                        MainTable.AddCell(cellHead);
                                    }
                                }
                                else
                                {
                                    cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                    cellHead.Colspan = 2;
                                    cellHead.BorderWidth = 0;
                                    cell.PaddingRight = 5F;
                                    cell.PaddingBottom = 0.5F;
                                    cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                    MainTable.AddCell(cellHead);
                                }
                                #endregion
                                #region Right
                                signUrl = payDefaultSett.Where(m => m.FieldValue2 == "SIGNATURE_RIGHT_URL").Select(m => m.FieldValue3).FirstOrDefault();
                                signText3 = payDefaultSett.Where(m => m.FieldValue2 == "SIGNATURE_RIGHT_TEXT").Select(m => m.FieldValue3).FirstOrDefault();
                                if (!String.IsNullOrEmpty(signUrl))
                                {
                                    try
                                    {
                                        iTextSharp.text.Image jpg = iTextSharp.text.Image.GetInstance(signUrl);
                                        jpg.ScaleAbsolute(100F, 25F);
                                        PdfPCell signCell = new PdfPCell(jpg);
                                        signCell.Colspan = 2;
                                        signCell.BorderWidth = 0;
                                        signCell.PaddingRight = 5F;
                                        signCell.PaddingBottom = 0.5F;
                                        signCell.HorizontalAlignment = Element.ALIGN_CENTER;
                                        MainTable.AddCell(signCell);
                                    }
                                    catch (Exception)
                                    {
                                        cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                        cellHead.Colspan = 2;
                                        cellHead.BorderWidth = 0;
                                        cell.PaddingRight = 5F;
                                        cell.PaddingBottom = 0.5F;
                                        cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                        MainTable.AddCell(cellHead);
                                    }
                                }
                                else
                                {
                                    cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                    cellHead.Colspan = 2;
                                    cellHead.BorderWidth = 0;
                                    cell.PaddingRight = 5F;
                                    cell.PaddingBottom = 0.5F;
                                    cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                    MainTable.AddCell(cellHead);
                                }
                                #endregion
                                if (payDefaultSett.Any() && payDefaultSett.Any(m => m.FieldValue2 == "SIGNATURE_LEFT_URL" || m.FieldValue2 == "SIGNATURE_CENTER_URL" || m.FieldValue2 == "SIGNATURE_RIGHT_URL"))
                                {
                                    cellHead = new PdfPCell(new Phrase(signText1, fntTableFont));
                                    cellHead.Colspan = 2;
                                    cellHead.BorderWidth = 0;
                                    cell.PaddingRight = 5F;
                                    cell.PaddingBottom = 0.5F;
                                    cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                    MainTable.AddCell(cellHead);
                                    cellHead = new PdfPCell(new Phrase(signText2, fntTableFont));
                                    cellHead.Colspan = 2;
                                    cellHead.BorderWidth = 0;
                                    cell.PaddingRight = 5F;
                                    cell.PaddingBottom = 0.5F;
                                    cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                    MainTable.AddCell(cellHead);
                                    cellHead = new PdfPCell(new Phrase(signText3, fntTableFont));
                                    cellHead.Colspan = 2;
                                    cellHead.BorderWidth = 0;
                                    cell.PaddingRight = 5F;
                                    cell.PaddingBottom = 0.5F;
                                    cellHead.HorizontalAlignment = Element.ALIGN_CENTER;
                                    MainTable.AddCell(cellHead);
                                }
                            }
                            #endregion
                            #endregion

                            #region Monthly Remarks


                            var d = lstdataOtherSett.Where(a => a.PayConNo == catPayConfigNo).FirstOrDefault();
                            if (d != null)
                            {
                                if (d.MonthlyRemarks != null && d.MonthlyRemarks != "")
                                {
                                    List<MonthlyRemarks> obj = new List<MonthlyRemarks>();
                                    obj = Newtonsoft.Json.JsonConvert.DeserializeObject<List<MonthlyRemarks>>(d.MonthlyRemarks);
                                    string tmonthlyRemarks = "";
                                    var tmonth = (int)myQueItem.month;
                                    var tyear = myQueItem.year;
                                    foreach (var item in obj)
                                    {
                                        if (tyear.ToString() == item.Year)
                                        {
                                            if (tmonth.ToString() == item.Month)
                                            {
                                                tmonthlyRemarks = item.Remark;
                                                break;
                                            }
                                        }
                                    }

                                    if (tmonthlyRemarks != "")
                                    {

                                        //pdf start
                                        cellHead = new PdfPCell(new Phrase("Payslip Notes:", fntTableFont));
                                        cellHead.Colspan = 1;
                                        cellHead.BorderWidth = 0;
                                        cellHead.BorderWidthBottom = 0;
                                        //cellHead.BorderWidthTop = 0.5F;
                                        cellHead.BorderWidthTop = 0;
                                        cellHead.BorderWidthLeft = 0;
                                        cellHead.BorderWidthRight = 0;
                                        cellHead.PaddingLeft = 10F;
                                        //MainTable.AddCell(cellHead);

                                        cellHead = new PdfPCell(new Phrase("", fntTableFontBold));
                                        cellHead.Colspan = 1;
                                        cellHead.BorderWidth = 0;
                                        cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                                        cellHead.BorderWidthBottom = 0;
                                        cellHead.BorderWidthTop = 0;
                                        cellHead.BorderWidthLeft = 0;
                                        cellHead.BorderWidthRight = 0;
                                        cellHead.PaddingRight = 2F;
                                        //MainTable.AddCell(cellHead);

                                        cellHead = new PdfPCell(new Phrase("", fntTableFont));
                                        cellHead.Colspan = 3;
                                        cellHead.BorderWidth = 0;
                                        cellHead.HorizontalAlignment = Element.ALIGN_RIGHT;
                                        cellHead.BorderWidthBottom = 0;
                                        cellHead.BorderWidthTop = 0;
                                        cellHead.BorderWidthLeft = 0;
                                        cellHead.BorderWidthRight = 0;
                                        //MainTable.AddCell(cellHead);


                                        cellHead = new PdfPCell(new Phrase(tmonthlyRemarks, fntTableFont));
                                        cellHead.Colspan = 10;
                                        //cellHead.BorderWidthTop = 0.5F;
                                        cellHead.BorderWidthTop = 0;
                                        cellHead.BorderWidth = 0;
                                        cellHead.HorizontalAlignment = Element.ALIGN_LEFT;
                                        MainTable.AddCell(cellHead);

                                        //pdf ends

                                    }

                                }
                            }


                            #endregion

                            PdfPCell cellOut = new PdfPCell();
                            cellOut.Colspan = 6;
                            cellOut.BorderColor = BaseColor.BLACK;
                            cellOut.BorderWidthBottom = 1F;
                            cellOut.BorderWidthTop = 1F;
                            cellOut.BorderWidthLeft = 1F;
                            cellOut.BorderWidthRight = 1F;
                            cellOut.AddElement(MainTable);
                            cellOut.Padding = 3f;
                            OuterTable.AddCell(cellOut);

                            #region Benefit Data
                            int BenMonth = (int)month;
                            BenMonth++;
                            int BenYear = int.Parse(Year);
                            if (BenMonth == 13)
                            {
                                BenMonth = 1;
                                BenYear++;
                            }
                            if (dtBenComp.Rows.Count > 0)
                            {
                                PdfPTable BenTable = new PdfPTable(3);
                                BenTable.WidthPercentage = 100;
                                PdfPCell Bencell = new PdfPCell(new Phrase("Reimbursement", fntTableFontBold));
                                Bencell.BorderWidth = 0.5F;
                                Bencell.Colspan = 3;
                                BenTable.AddCell(Bencell);
                                Bencell = new PdfPCell(new Phrase("Benefit Description", fntTableFontBold));
                                Bencell.BorderWidth = 0;
                                Bencell.BorderWidthBottom = 0.5F;
                                Bencell.BorderWidthLeft = 0.5F;
                                BenTable.AddCell(Bencell);
                                Bencell = new PdfPCell(new Phrase("Amount(Rs.)", fntTableFontBold));
                                Bencell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                Bencell.BorderWidth = 0;
                                Bencell.BorderWidthBottom = 0.5F;
                                BenTable.AddCell(Bencell);
                                Bencell = new PdfPCell(new Phrase("Balance(Rs.)", fntTableFontBold));
                                Bencell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                Bencell.BorderWidth = 0;
                                Bencell.BorderWidthBottom = 0.5F;
                                Bencell.BorderWidthRight = 0.5F;
                                BenTable.AddCell(Bencell);
                                var yearBen = int.Parse(Year);
                                var BenData = ctx.BenPost.Where(m => m.CompanyId == companyId && m.EmployeeId == emp.Id && m.PostMonth == month && m.Year == yearBen).ToList();
                                double TotAmt = 0, TotBal = 0;
                                int tempCategory = empList.Where(a => a.Id == emp.Id).Select(a => a.Category).FirstOrDefault();
                                var lstData = lstTempBenCompData.Where(a => a.CategoryId == tempCategory).ToList();
                                foreach (var item in lstData)
                                {
                                    //int FinNo = int.Parse(dtBenComp.Rows[i]["FinNo"].ToString());
                                    //string BenCode = dtBenComp.Rows[i]["FieldName"].ToString();
                                    //ReimbursementType ReimbursementType = (ReimbursementType)Enum.Parse(typeof(ReimbursementType), dtBenComp.Rows[i]["ReimbursementType"].ToString());
                                    int FinNo = item.FinNo;
                                    string BenCode = item.FieldName.ToString();
                                    string BenCodeLabelName = item.LabelName.ToString();
                                    ReimbursementType ReimbursementType = (ReimbursementType)Enum.Parse(typeof(ReimbursementType), item.ReimbursementType.ToString());
                                    double OpBal = 0;
                                    if (ReimbursementType == ReimbursementType.Annual)
                                    {
                                        var Data = ctx.BenOpeningBalance.Where(m => m.EmployeeId == emp.Id && m.CompanyId == companyId && m.BenCode == BenCode && m.FinNo == FinNo && m.OPMonth == (InputMonths)BenMonth && m.OPYear == BenYear).Select(m => m.FixedCurrEnt).FirstOrDefault();
                                        if (Data != null)
                                        {
                                            OpBal = Data;
                                        }
                                    }
                                    else
                                    {
                                        var Data = ctx.BenOpeningBalance.Where(m => m.EmployeeId == emp.Id && m.CompanyId == companyId && m.BenCode == BenCode && m.FinNo == FinNo && m.OPMonth == (InputMonths)BenMonth && m.OPYear == BenYear).Select(m => m.OPCummulativeAmt).FirstOrDefault();
                                        if (Data != null)
                                        {
                                            OpBal = Data;
                                        }
                                    }
                                    Bencell = new PdfPCell(new Phrase(BenCodeLabelName, fntTableFont));
                                    Bencell.BorderWidth = 0;
                                    Bencell.BorderWidthLeft = 0.5F;
                                    BenTable.AddCell(Bencell);
                                    double BenCodeAmount = BenData.Where(m => m.BenefitCode == BenCode).Select(m => m.Amount).FirstOrDefault();
                                    Bencell = new PdfPCell(new Phrase(BenCodeAmount.ToString("0.00"), fntTableFont));
                                    Bencell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                    Bencell.BorderWidth = 0;
                                    BenTable.AddCell(Bencell);
                                    Bencell = new PdfPCell(new Phrase(OpBal.ToString("0.00"), fntTableFont));
                                    Bencell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                    Bencell.BorderWidth = 0;
                                    Bencell.BorderWidthRight = 0.5F;
                                    BenTable.AddCell(Bencell);
                                    TotAmt += BenCodeAmount;
                                    TotBal += OpBal;
                                }
                                Bencell = new PdfPCell(new Phrase("   ", fntTableFontBold));
                                Bencell.BorderWidth = 0;
                                Bencell.BorderWidthLeft = 0.5F;
                                Bencell.BorderWidthTop = 0.5F;
                                Bencell.BorderWidthBottom = 0.5F;
                                BenTable.AddCell(Bencell);
                                Bencell = new PdfPCell(new Phrase(TotAmt.ToString("0.00"), fntTableFontBold));
                                Bencell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                Bencell.BorderWidth = 0;
                                Bencell.BorderWidthTop = 0.5F;
                                Bencell.BorderWidthBottom = 0.5F;
                                BenTable.AddCell(Bencell);
                                Bencell = new PdfPCell(new Phrase(TotBal.ToString("0.00"), fntTableFontBold));
                                Bencell.HorizontalAlignment = Element.ALIGN_RIGHT;
                                Bencell.BorderWidth = 0;
                                Bencell.BorderWidthRight = 0.5F;
                                Bencell.BorderWidthTop = 0.5F;
                                Bencell.BorderWidthBottom = 0.5F;
                                BenTable.AddCell(Bencell);
                                cellOut = new PdfPCell();
                                cellOut.Colspan = 6;
                                cell.PaddingTop = 100;
                                cellOut.BorderWidth = 0;
                                cellOut.AddElement(BenTable);
                                OuterTable.AddCell(cellOut);
                            }
                            #endregion

                            if (flagPerPage)
                            {
                                if (count == 0)
                                {
                                    document = new Document(PageSize.A4, 10, 10, 25, 25);
                                    output = new MemoryStream();
                                    writer = PdfWriter.GetInstance(document, output);
                                    document.Open();
                                }
                                float a4sizePageHeight = document.PageSize.Height;
                                PdfPTable TempOuterTable = new PdfPTable(6);
                                var tempdocument = new Document(PageSize.A4, 10, 10, 25, 25);
                                var tempoutput = new MemoryStream();
                                var tempwriter = PdfWriter.GetInstance(tempdocument, tempoutput);
                                tempdocument.Open();
                                TempOuterTable = OuterTable;
                                tempdocument.Add(TempOuterTable);
                                tempdocument.Close();
                                float mainTableHeight = OuterTable.GetRowHeight(0);
                                if (count % paySlipPerPage == 0)
                                {
                                    document.NewPage();
                                }
                                cnt = 0;
                                if (SchemaName != "JaiBa393a76fbb0")
                                {
                                    OuterTable.SpacingAfter = 5;
                                }
                                else
                                {
                                    OuterTable.SpacingAfter = 60;
                                }
                                count++;
                            }
                            else
                            {
                                document = new Document(PageSize.A4, 10, 10, 25, 25);
                                output = new MemoryStream();
                                writer = PdfWriter.GetInstance(document, output);
                                document.Open();
                            }
                            #region Header Design from Template
                            if (payHeadSett)
                            {
                                var headDesignMain = ctx.EmailTemplates.Where(m => m.Id == templateId).Select(m => m.Body).FirstOrDefault();

                                // Replace Placeholders
                                headDesignMain = headDesignMain.Replace("[COMPNAME|PH]", dataComp.CompanyName);
                                headDesignMain = headDesignMain.Replace("[COMPADDRESS|PH]", dataComp.Address);
                                headDesignMain = headDesignMain.Replace("[COMPCITY|PH]", dataComp.City);
                                headDesignMain = headDesignMain.Replace("[COMPSTATE|PH]", dataComp.State);
                                headDesignMain = headDesignMain.Replace("[COMPPINCODE|PH]", dataComp.PinCode);
                                headDesignMain = headDesignMain.Replace("[COMPTELEPHONE|PH]", dataComp.Telephone);
                                headDesignMain = headDesignMain.Replace("[MONTH|PH]", month.ToString());
                                headDesignMain = headDesignMain.Replace("[YEAR|PH]", Year);
                                string headDesign = "";
                                headDesign = ReplacePlaceHoldersEmployeeHR(SchemaName, empColumns, headDesignMain, companyId, emp.Id, emp, dataComp, lstPayConfigNo, empConfig, dataCombo, customDataEmp);
                                headDesign = ReplacePlaceHoldersEarningsPayslip(SchemaName, companyId, earngColumns, dedColumns, emp, headDesign, PayDate);
                                //headDesign = empService.ReplacePlaceHoldersDeductionsHR(SchemaName, companyId, dedColumns, emp, headDesign);
                                // Replacing Harcoded Earning & Deduction Fields
                                headDesign = headDesign.Replace("[NETPAY|EARNINGS]", "0");
                                headDesign = headDesign.Replace("[GROSS|EARNINGS]", "0");
                                headDesign = headDesign.Replace("[BENNETPAY|EARNINGS]", "0");
                                headDesign = headDesign.Replace("[WAGELIMIT|EARNINGS]", "0");
                                headDesign = headDesign.Replace("[ESIGROSS|EARNINGS]", "0");
                                headDesign = headDesign.Replace("[PTGROSS|EARNINGS]", "0");
                                headDesign = headDesign.Replace("[NETPAY|DEDUCTIONS]", "0");
                                headDesign = headDesign.Replace("[EPFAMOUNT|DEDUCTIONS]", "0");
                                headDesign = headDesign.Replace("[ESICONAMOUNT|DEDUCTIONS]", "0");
                                headDesign = headDesign.Replace("[FPFAMOUNT|DEDUCTIONS]", "0");
                                headDesign = headDesign.Replace("[VPFAMOUNT|DEDUCTIONS]", "0");
                                headDesign = headDesign.Replace("[TD|DEDUCTIONS]", "0");
                                string headerContents = "<div style='font-family: " + payFontSett + ";padding: 0px 0px 0px 1px;'>";
                                headerContents += headDesign;
                                headerContents += "</div>";
                                StringReader sr = new StringReader(headerContents);
                                //var data = HttpUtility.HtmlEncode(headerContents);
                                //StringReader sr = new StringReader(data);
                                iTextSharp.tool.xml.XMLWorkerHelper.GetInstance().ParseXHtml(writer, document, sr);
                                //var parsedHtmlElements = HTMLWorker.ParseToList(new StringReader(headerContents), null);
                                //foreach (var htmlElement in parsedHtmlElements)
                                //{
                                //    document.Add(htmlElement as IElement);
                                //}
                            }
                            #endregion
                            document.Add(OuterTable);
                            if (!flagPerPage)
                            {
                                document.Close();
                            }
                            bool isexist = System.IO.Directory.Exists(payslipPath.Replace('\\', '/') + "/" + userId + "/Payslips");
                            if (!isexist)
                            {
                                System.IO.Directory.CreateDirectory(payslipPath.Replace('\\', '/') + "/" + userId + "/Payslips");
                            }

                            if (!flagPerPage && passFlag != 0)
                            {
                                using (MemoryStream memoryStream = new MemoryStream())
                                {
                                    byte[] bytes = output.ToArray();
                                    if (!string.IsNullOrEmpty(WorkSheet))
                                    {
                                        //var outputNew = Worksheet_Payslip(myQueItem, SchemaName, companyId, userId, emp.Id);
                                        var outputNew = Worksheet_Payslip(myQueItem, SchemaName, companyId, userId, emp.Id, empConfig, dataCombo, lstPayConfigNo, companyDetail, finSetData,
    lstTaxRepSet, taxConfigData, taxIncomeMatchData, lstEarnigC, lstTaxPreIncome, taxSlabData, TaxIncomeQuery.Where(a => a.EmployeeId == emp.Id).ToList(),
    lstTaxCalculation.Where(a => a.EmployeeId == emp.Id).ToList(),
    empList.Where(a => a.Id == emp.Id).ToList(),
    data2_worksheet, data3_worksheet, data_worksheet, lstPayConfigNo, lstEmpTransCustomDetails);

                                        if (outputNew.Length == 0)
                                            bytes = output.ToArray();

                                        else
                                        {
                                            List<byte[]> pdfByteContent = new List<byte[]>();
                                            pdfByteContent.Add(output.ToArray());
                                            pdfByteContent.Add(outputNew);
                                            var combineData = concatAndAddContent(pdfByteContent);
                                            bytes = combineData;
                                        }
                                    }
                                    else
                                    {
                                        bytes = output.ToArray();
                                    }
                                    memoryStream.Close();
                                    //if (emp.Code == "615015")
                                    //{
                                    //    query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,'EMP4 615015" + emp.Code + " ')";
                                    //    dBManager.Insert(query1, System.Data.CommandType.Text);

                                    //}
                                    using (MemoryStream input = new MemoryStream(bytes))
                                    {
                                        using (MemoryStream output1 = new MemoryStream())
                                        {
                                            string password = "";
                                            switch (passFlag)
                                            {
                                                case PayslipPassword.PanCard:
                                                    if (emp.PanNo != null)
                                                    {
                                                        password = emp.PanNo;
                                                    }
                                                    break;
                                                case PayslipPassword.EmpCodeDob:
                                                    if (emp.DateOfBirth != null)
                                                    {
                                                        password = emp.Code + emp.DateOfBirth.Value.ToString("ddMMyyyy");
                                                    }
                                                    break;
                                                case PayslipPassword.EmpCode:
                                                    password = emp.Code;
                                                    break;
                                                case PayslipPassword.Dob:
                                                    if (emp.DateOfBirth != null)
                                                    {
                                                        password = emp.DateOfBirth.Value.ToString("ddMMyyyy");
                                                    }
                                                    break;
                                                case PayslipPassword.PanCardDob:
                                                    if (emp.DateOfBirth != null)
                                                    {
                                                        password = emp.PanNo + emp.DateOfBirth.Value.ToString("ddMMyyyy");
                                                    }
                                                    break;
                                            }
                                            PdfReader reader = new PdfReader(input);
                                            PdfEncryptor.Encrypt(reader, output1, true, password, password, PdfWriter.ALLOW_SCREENREADERS);

                                            //if (emp.Code == "615015")
                                            //{
                                            //    query1 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue4) values(290,999,'EMP6 615015" + emp.Code + " ')";
                                            //    dBManager.Insert(query1, System.Data.CommandType.Text);

                                            //}
                                            bytes = output1.ToArray();
                                            System.IO.File.WriteAllBytes(payslipPath.Replace('\\', '/') + "/" + userId + "/Payslips/" + emp.Code.Replace('/', '_').ToString() + ".pdf", output1.ToArray());
                                            ms = new MemoryStream(bytes);
                                        }
                                    }
                                }
                            }
                            else
                            {
                                //byte[] buffer = new byte[16 * 1024];
                                if (!string.IsNullOrEmpty(WorkSheet))
                                {
                                    //var outputNew = Worksheet_Payslip(myQueItem, SchemaName, companyId, userId, emp.Id);
                                    var outputNew = Worksheet_Payslip(myQueItem, SchemaName, companyId, userId, emp.Id, empConfig, dataCombo, lstPayConfigNo, companyDetail, finSetData,
      lstTaxRepSet, taxConfigData, taxIncomeMatchData, lstEarnigC, lstTaxPreIncome, taxSlabData, TaxIncomeQuery.Where(a => a.EmployeeId == emp.Id).ToList(),
      lstTaxCalculation.Where(a => a.EmployeeId == emp.Id).ToList(),
      empList.Where(a => a.Id == emp.Id).ToList(),
      data2_worksheet, data3_worksheet, data_worksheet, lstPayConfigNo, lstEmpTransCustomDetails.Where(a => a.Id == emp.Id).ToList());

                                    List<byte[]> pdfByteContent = new List<byte[]>();
                                    pdfByteContent.Add(output.ToArray());
                                    if (outputNew.Length > 0)
                                    {
                                        pdfByteContent.Add(outputNew);
                                    }
                                    var combineData = concatAndAddContent(pdfByteContent);
                                    if (!flagPerPage)
                                    {
                                        System.IO.File.WriteAllBytes(payslipPath.Replace('\\', '/') + "/" + userId + "/Payslips/" + emp.Code.Replace('/', '_').ToString() + ".pdf", combineData);
                                    }
                                    ms = new MemoryStream(combineData);
                                }
                                else
                                {
                                    if (!flagPerPage)
                                    {
                                        System.IO.File.WriteAllBytes(payslipPath.Replace('\\', '/') + "/" + userId + "/Payslips/" + emp.Code.Replace('/', '_').ToString() + ".pdf", output.ToArray());
                                    }
                                    ms = new MemoryStream(output.ToArray());
                                }
                            }
                            #region Send Mail
                            //var checkEmptrans1 = empTranscData.Any(a => a.EmployeeId == emp.Id && a.CompanyId == companyId && a.Month == month && a.Year == intYear);
                            var checkEmptrans1 = empTranscData.Any(a => a.EmployeeId == emp.Id);
                            if (!flagPerPage && SubmitType == "Send Email")
                            {
                                // Send Emails
                                string Email = "";
                                if (recEmail == ReceiversEmail.EmailID)
                                {
                                    Email = emp.Email;
                                }
                                else if (recEmail == ReceiversEmail.PersonalEmailID)
                                {
                                    Email = emp.PersonalEmail;
                                }
                                if (!string.IsNullOrEmpty(Email))
                                {
                                    if (checkEmptrans1)
                                    {
                                        try
                                        {
                                            List<Attachment> list = new List<Attachment>();
                                            Attachment file = new Attachment(ms, emp.Code.ToString() + ".pdf");
                                            list.Add(file);
                                            EmailBody = ReplacePlaceHoldersEmployeeHROptimize(SchemaName, companyId, empConfig, empColumns, emp, EmailBodyMain, int.Parse(Month), int.Parse(Year), lstPayConfigNo, dataCombo, empTransCustomData, empList, dataComp, customData);
                                            var emailtempData = new EmailTemplates();

                                            emailtempData.Body = EmailBody;
                                            emailtempData.Subject = EmailSubject;
                                            emailtempData.lstAttachments = list;

                                            if (SchemaName == "Smollfe37426078" || SchemaName == "Greyt56af30a136")
                                            {
                                                SendGrid.Helpers.Mail.Attachment file1 = new SendGrid.Helpers.Mail.Attachment();
                                                file1.Content = Convert.ToBase64String(ms.ToArray());
                                                file1.Filename = emp.Code.ToString() + ".pdf";
                                                file1.Type = "application/pdf";
                                                var emailtempDataSmollean = new EmailTemplates();
                                                emailtempDataSmollean.Body = EmailBody;
                                                emailtempDataSmollean.Subject = EmailSubject;
                                                emailtempDataSmollean.Attachment = file1;
                                                Tuple<string, string> tuple = await _mailBS.sendMailWithPayslipLogSmollean(smtpsetting, emailtempDataSmollean, Email, EmailSubject, emp.Code, emp.FName + " " + emp.LName, userId, ListFailedLog, emp.Id);                                            //mailService.SendReportMail(SchemaName, companyId, Email, EmailSubject, EmailBody, list);
                                                                                                                                                                                                                                                                                                      //EmpsNotReceivedMail1.Add(tuple.Item1.ToString());

                                                if (!string.IsNullOrEmpty(tuple.Item1))
                                                {
                                                    if (string.IsNullOrEmpty(EmpsNotReceivedMail))
                                                        EmpsNotReceivedMail = tuple.Item1;
                                                    else
                                                        EmpsNotReceivedMail += "," + tuple.Item1;
                                                }

                                                if (!string.IsNullOrEmpty(tuple.Item2))
                                                {
                                                    if (string.IsNullOrEmpty(EmpsReceivedMail))
                                                        EmpsReceivedMail = tuple.Item2;
                                                    else
                                                        EmpsReceivedMail += "," + tuple.Item2;
                                                }
                                            }
                                            else
                                            {
                                                _mailBS.sendMailWithPayslipLog(smtpsetting, emailtempData, Email, EmailSubject, emp.Code, emp.FName + " " + emp.LName, userId, ListFailedLog, emp.Id, ref EmpsReceivedMail, ref EmpsNotReceivedMail);                                            //mailService.SendReportMail(SchemaName, companyId, Email, EmailSubject, EmailBody, list);
                                            }
                                            countEmpEmail++;

                                            EmailBody = "";
                                        }
                                        catch (Exception e)
                                        {
                                            if (string.IsNullOrEmpty(EmpsNotReceivedMail))
                                                EmpsNotReceivedMail = emp.Id.ToString();
                                            else
                                                EmpsNotReceivedMail += "," + emp.Id.ToString();
                                            PayslipFailedMailLog failedlog = new PayslipFailedMailLog();
                                            failedlog.EmployeeCode = emp.Code;
                                            failedlog.CreatedDate = DateTime.Now;
                                            failedlog.EmployeeName = emp.FName + " " + emp.LName;
                                            failedlog.UserId = userId;
                                            failedlog.Reason = e.Message;
                                            ListFailedLog.Add(failedlog);
                                        }
                                    }
                                    else
                                    {
                                        if (string.IsNullOrEmpty(EmpsNotReceivedMail))
                                            EmpsNotReceivedMail = emp.Id.ToString();
                                        else
                                            EmpsNotReceivedMail += "," + emp.Id.ToString();
                                        PayslipFailedMailLog failedlog = new PayslipFailedMailLog();
                                        failedlog.EmployeeCode = emp.Code;
                                        failedlog.CreatedDate = DateTime.Now;
                                        failedlog.EmployeeName = emp.FName + " " + emp.LName;
                                        failedlog.UserId = userId;
                                        failedlog.Reason = "Payroll Process is not done!";
                                        ListFailedLog.Add(failedlog);
                                    }
                                }
                                else
                                {
                                    if (string.IsNullOrEmpty(EmpsNotReceivedMail))
                                        EmpsNotReceivedMail = emp.Id.ToString();
                                    else
                                        EmpsNotReceivedMail += "," + emp.Id.ToString();
                                    PayslipFailedMailLog failedlog = new PayslipFailedMailLog();
                                    failedlog.EmployeeCode = emp.Code;
                                    failedlog.CreatedDate = DateTime.Now;
                                    failedlog.EmployeeName = emp.FName + " " + emp.LName;
                                    failedlog.Reason = "Email ID not set for the Employee";
                                    failedlog.UserId = userId;
                                    ListFailedLog.Add(failedlog);
                                    empListNoEmail.Add(emp);
                                    if (string.IsNullOrEmpty(EmpsNotReceivedMail))
                                        EmpsNotReceivedMail = emp.Id.ToString();
                                    else
                                        EmpsNotReceivedMail += "," + emp.Id.ToString();
                                }
                            }
                            #endregion

                            #region Send Whatsapp
                            if (!flagPerPage && SubmitType == "Send WhatsApp")
                            {
                                if (checkEmptrans1)
                                {
                                    try
                                    {
                                        BlobStorageService blobStorageService = new BlobStorageService();
                                        var fileBytes = ms.ToArray();
                                        var FileName = emp.Code.Replace("/", "") + "_" + enumMonth.ToString() + "_" + year + "_" + DateTime.Now.ToString("yyyyMMddHHmmssffff");
                                        var blobURL = await blobStorageService.UploadFileToBlob(FileName, fileBytes, "application/pdf", "whatsapppayslip", SchemaName);

                                        //var empName = emp.FName + " " + emp.LName;
                                        var empName = emp.FName;
                                        var CompanyName = wpCompanyName;
                                        var payslip = "Payslip";
                                        var mobileNo = emp.PhoneNo;
                                        var wAMessageLogs = await _statutoryReportRepository.sendPayslipToWhatsApp(SchemaName, companyId, emp.Id, empName, payslip, month.ToString(), year, CompanyName, blobURL, mobileNo, emp.Code, userId);
                                        if (wAMessageLogs.payslipFailedMailLogs.Count > 0 || wAMessageLogs.WAMessageLogs.Count > 0)
                                        {
                                            if (wAMessageLogs.payslipFailedMailLogs.Count > 0)
                                                if (string.IsNullOrEmpty(EmpsNotReceivedWhatsApp))
                                                    EmpsNotReceivedWhatsApp = emp.Id.ToString();
                                                else
                                                {
                                                    if (!EmpsNotReceivedWhatsApp.Contains(emp.Id.ToString()))
                                                        EmpsNotReceivedWhatsApp += "," + emp.Id.ToString();
                                                }

                                            if (wAMessageLogs.WAMessageLogs.Count > 0)
                                            {
                                                var response = wAMessageLogs.WAMessageLogs[0].Response;
                                                if (response.Contains("error"))
                                                {
                                                    if (string.IsNullOrEmpty(EmpsNotReceivedWhatsApp))
                                                        EmpsNotReceivedWhatsApp = emp.Id.ToString();
                                                    else
                                                    {
                                                        if (!EmpsNotReceivedWhatsApp.Contains(emp.Id.ToString()))
                                                            EmpsNotReceivedWhatsApp += "," + emp.Id.ToString();
                                                    }
                                                }
                                                else
                                                {
                                                    if (string.IsNullOrEmpty(EmpsReceivedWhatsApp))
                                                        EmpsReceivedWhatsApp = emp.Id.ToString();
                                                    else
                                                    {
                                                        if (!EmpsReceivedWhatsApp.Contains(emp.Id.ToString()))
                                                            EmpsReceivedWhatsApp += "," + emp.Id.ToString();
                                                    }
                                                }
                                                countEmpWhatsapp++;
                                            }


                                            failedMailLogs.AddRange(wAMessageLogs.payslipFailedMailLogs);
                                            wAMessage.AddRange(wAMessageLogs.WAMessageLogs);
                                        }
                                        else
                                        {
                                            if (string.IsNullOrEmpty(EmpsNotReceivedWhatsApp))
                                                EmpsNotReceivedWhatsApp = emp.Id.ToString();
                                            else
                                            {
                                                if (!EmpsNotReceivedWhatsApp.Contains(emp.Id.ToString()))
                                                    EmpsNotReceivedWhatsApp += "," + emp.Id.ToString();
                                            }
                                        }

                                    }
                                    catch (Exception ex)
                                    {
                                        if (string.IsNullOrEmpty(EmpsNotReceivedWhatsApp))
                                            EmpsNotReceivedWhatsApp = emp.Id.ToString();
                                        else
                                            EmpsNotReceivedWhatsApp += "," + emp.Id.ToString();
                                        PayslipFailedMailLog failedlog = new PayslipFailedMailLog();
                                        failedlog.EmployeeCode = emp.Code;
                                        failedlog.CreatedDate = DateTime.Now;
                                        failedlog.EmployeeName = emp.FName + " " + emp.LName;
                                        failedlog.Reason = ex.Message;
                                        failedlog.UserId = userId;
                                        failedMailLogs.Add(failedlog);
                                        //empListNoEmail.Add(emp);
                                    }
                                }
                            }

                            #endregion
                            #endregion
                        }
                    }

                    if (flagPerPage)
                    {
                        document.Close();
                        System.IO.File.WriteAllBytes(payslipPath.Replace('\\', '/') + "/" + userId + "/Payslips/MultiPayslip.pdf", output.ToArray());
                    }
                    #region Send Mail - Message
                    if (SubmitType == "Send Email")
                    {
                        string Message = "";
                        if (empList.Count == 1)
                        {
                            EmpId = empList[0].Id;
                            //var checkEmptrans = empTranscData.Any(a => a.EmployeeId == EmpId && a.CompanyId == companyId && a.Month == month && a.Year == intYear);
                            var checkEmptrans = empTranscData.Any(a => a.EmployeeId == EmpId);
                            if (countEmpEmail == 1)
                            {
                                Message = "Email Sent Successfully";
                            }
                            else if (!checkEmptrans)
                            {
                                Message = "Payroll Process is not done!";
                            }
                            else
                            {
                                Message = "Email ID not set for the Employee";
                            }
                        }
                        else
                        {
                            Message = "Email Sent to " + countEmpEmail + " out of " + empList.Count + " Employees";
                        }
                        //payslip mail log
                        PayslipMailLog mod = new PayslipMailLog();
                        mod.CompanyId = companyId;
                        mod.Json = JsonConvert.SerializeObject(myQueItem);
                        mod.CreatedDate = DateTime.Now;
                        mod.EmpsNotReceivedMail = EmpsNotReceivedMail;
                        mod.EmpsReceivedMail = EmpsReceivedMail;
                        mod.ParentId = 0;
                        mod.UserId = userId;
                        if (ListFailedLog.Count > 0)
                            mod.FailedLogJson = JsonConvert.SerializeObject(ListFailedLog);
                        if (myQueItem.ParentPayslipLogId != 0)
                            mod.ParentId = myQueItem.ParentPayslipLogId;
                        ctx.PayslipMailLog.Add(mod);
                        ctx.SaveChanges();
                        Msg = "mail|" + Message;
                        myQueItem.PayslipLogId = mod.Id;
                        myQueItem.MailSendBool = true;
                    }
                    else if (SubmitType == "Send WhatsApp")
                    {
                        // whatsapp logs bulk
                        if (wAMessage.Count > 0)
                        {
                            using (var masterDB = new MasterDBContext())
                            {
                                masterDB.WAMessageLog.AddRange(wAMessage);
                                masterDB.SaveChanges();
                            }
                        }
                        //payslip mail log
                        PayslipMailLog mod = new PayslipMailLog();
                        mod.CompanyId = companyId;
                        mod.Json = JsonConvert.SerializeObject(myQueItem);
                        mod.CreatedDate = DateTime.Now;
                        mod.EmpsNotReceivedMail = EmpsNotReceivedWhatsApp;
                        mod.EmpsReceivedMail = EmpsReceivedWhatsApp;
                        mod.ParentId = 0;
                        mod.UserId = userId;
                        if (failedMailLogs.Count > 0)
                            mod.FailedLogJson = JsonConvert.SerializeObject(failedMailLogs);
                        if (myQueItem.ParentPayslipLogId != 0)
                            mod.ParentId = myQueItem.ParentPayslipLogId;
                        ctx.PayslipMailLog.Add(mod);
                        ctx.SaveChanges();
                        if (failedMailLogs.Count > 0)
                        {
                            Msg = "whatsapp| WhatsaApp Sent to " + countEmpWhatsapp + " out of " + empList.Count + " Employees"; ;
                        }
                        else
                        {
                            Msg = "whatsapp| Whatsapp Sent Successfully";
                        }
                        myQueItem.PayslipLogId = mod.Id;
                        myQueItem.MailSendBool = true;
                    }

                    #endregion
                }
                if (SubmitType == "Download")
                {
                    Msg = "completed|done";
                }
                if (SubmitType == "Upload To Azure")
                {
                    Msg = "completed|done";
                }
                var timeTaken = timer.Elapsed;
                return Msg;
            }
            catch (Exception e)
            {
                var dbmanager = new DBConnectionManager(connectionString);
                var error = "PayslipCRASH " + e.Message + "___" + e.StackTrace ?? "";
                var currentDate = DateTime.Now;
                var query12 = "insert into Greyt56af30a136.payrollset(companyid,setname,fieldvalue1,fieldvalue2,fieldvalue3,fieldvalue4) values(290,999,'" + SchemaName + "','" + currentDate + "', '" + employeeid + "','" + error + "')";
                dbmanager.Insert(query12, System.Data.CommandType.Text);
                return "error |" + e.Message + " " + employeeid;

            }
        }
