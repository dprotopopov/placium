﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

// 
// This source code was auto-generated by xsd, Version=4.8.3928.0.
// 
namespace AS_APARTMENT_TYPES_2_251_07_04_01_01.xsd {
    using System.Xml.Serialization;
    
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true)]
    [System.Xml.Serialization.XmlRootAttribute(Namespace="", IsNullable=false)]
    public partial class APARTMENTTYPES {
        
        private APARTMENTTYPESAPARTMENTTYPE[] aPARTMENTTYPEField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlElementAttribute("APARTMENTTYPE")]
        public APARTMENTTYPESAPARTMENTTYPE[] APARTMENTTYPE {
            get {
                return this.aPARTMENTTYPEField;
            }
            set {
                this.aPARTMENTTYPEField = value;
            }
        }
    }
    
    /// <remarks/>
    [System.CodeDom.Compiler.GeneratedCodeAttribute("xsd", "4.8.3928.0")]
    [System.SerializableAttribute()]
    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.ComponentModel.DesignerCategoryAttribute("code")]
    [System.Xml.Serialization.XmlTypeAttribute(AnonymousType=true)]
    public partial class APARTMENTTYPESAPARTMENTTYPE {
        
        private string idField;
        
        private string nAMEField;
        
        private string sHORTNAMEField;
        
        private string dESCField;
        
        private System.DateTime uPDATEDATEField;
        
        private System.DateTime sTARTDATEField;
        
        private System.DateTime eNDDATEField;
        
        private bool iSACTIVEField;
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="integer")]
        public string ID {
            get {
                return this.idField;
            }
            set {
                this.idField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string NAME {
            get {
                return this.nAMEField;
            }
            set {
                this.nAMEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string SHORTNAME {
            get {
                return this.sHORTNAMEField;
            }
            set {
                this.sHORTNAMEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public string DESC {
            get {
                return this.dESCField;
            }
            set {
                this.dESCField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="date")]
        public System.DateTime UPDATEDATE {
            get {
                return this.uPDATEDATEField;
            }
            set {
                this.uPDATEDATEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="date")]
        public System.DateTime STARTDATE {
            get {
                return this.sTARTDATEField;
            }
            set {
                this.sTARTDATEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute(DataType="date")]
        public System.DateTime ENDDATE {
            get {
                return this.eNDDATEField;
            }
            set {
                this.eNDDATEField = value;
            }
        }
        
        /// <remarks/>
        [System.Xml.Serialization.XmlAttributeAttribute()]
        public bool ISACTIVE {
            get {
                return this.iSACTIVEField;
            }
            set {
                this.iSACTIVEField = value;
            }
        }
    }
}
